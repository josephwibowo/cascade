import {useCallback, useEffect, useRef, useState} from 'react';
import {api, type AccountRow} from '../api/client';

export type Chip = 'straightforward'|'actively_migrating'|'no_progress'|'strategic'|'contractual'|'technical_blocker';
export type Filters = {status?: string; risk?: string; chip?: Chip; q?: string};

const PAGE_SIZE = 200;
const chips: {id: Chip; label: string}[] = [
  {id:'straightforward',label:'Straightforward'},
  {id:'actively_migrating',label:'Actively migrating'},
  {id:'no_progress',label:'No progress'},
  {id:'strategic',label:'Strategic'},
  {id:'contractual',label:'Contractual'},
  {id:'technical_blocker',label:'Technical blocker'},
];

type AccountTableProps = {
  campaignId: string;
  refreshKey?: number;
  filters: Filters;
  onFiltersChange: (filters: Filters) => void;
  onSelect: (account: AccountRow) => void;
};

export default function AccountTable({campaignId, refreshKey = 0, filters, onFiltersChange, onSelect}: AccountTableProps) {
  const [items, setItems] = useState<AccountRow[]>([]);
  const [total, setTotal] = useState(0);
  const [facets, setFacets] = useState<Record<string, number>>({});
  const [searchInput, setSearchInput] = useState(filters.q || '');
  const [debouncedQ, setDebouncedQ] = useState(filters.q || '');
  const [scrollTop, setScrollTop] = useState(0);
  const [loading, setLoading] = useState(false);
  const [failed, setFailed] = useState(false);
  const [retry, setRetry] = useState(0);
  const requestNumber = useRef(0);
  const scrollElement = useRef<HTMLDivElement | null>(null);
  const loadedCount = useRef(0);
  const previousFilterKey = useRef<string | null>(null);
  const previousRefreshKey = useRef(refreshKey);
  const previousRetry = useRef(retry);

  // Dashboard can clear all filters while this component remains mounted.
  useEffect(() => {
    const next = filters.q || '';
    setSearchInput(next);
  }, [filters.q]);

  useEffect(() => {
    const timer = window.setTimeout(() => setDebouncedQ(searchInput), 250);
    return () => window.clearTimeout(timer);
  }, [searchInput]);

  const filterParams = useCallback((): Record<string, string> => {
    const params: Record<string, string> = {};
    if (filters.status) params.status = filters.status;
    if (filters.risk) params.risk = filters.risk;
    if (filters.chip) params.chip = filters.chip;
    if (debouncedQ) params.q = debouncedQ;
    return params;
  }, [debouncedQ, filters.chip, filters.risk, filters.status]);

  const loadPage = useCallback((offset: number, append: boolean, refreshCount = 0) => {
    const requestId = ++requestNumber.current;
    setLoading(true);
    setFailed(false);

    // A normal load or append needs one page. A dashboard refresh needs to
    // rebuild every page that was already loaded, since only replacing page
    // zero leaves rows that no longer match the filter in the table. Keep the
    // requests sequential and bounded by the prior loaded range so a poll
    // cannot fan out into an unbounded request storm.
    const targetCount = append ? PAGE_SIZE : refreshCount > 0 ? Math.max(PAGE_SIZE, refreshCount) : PAGE_SIZE;
    const pages: AccountRow[] = [];
    const seen = new Set<string>();
    let pageFacets: Record<string, number> = {};
    let serverTotal = 0;
    const maxPages = append || refreshCount <= 0 ? 1 : Math.max(1, Math.ceil(targetCount / PAGE_SIZE) + 1);

    const fetchPages = async () => {
      let nextOffset = offset;
      let fetchedPageCount = 0;
      while (fetchedPageCount < maxPages) {
        const value = await api.accounts(campaignId, filterParams(), PAGE_SIZE, nextOffset);
        if (requestId !== requestNumber.current) return;
        fetchedPageCount += 1;
        serverTotal = value.total;
        pageFacets = value.facets?.chips || {};
        for (const item of value.items) {
          if (!seen.has(item.account_id)) {
            seen.add(item.account_id);
            pages.push(item);
          }
        }

        // Appends intentionally fetch one page. Reset/filter loads also fetch
        // one page. Only a refresh walks the complete range that was already
        // visible, stopping at the new total when the result shrinks.
        if (append || refreshCount <= 0) break;
        const desired = Math.min(targetCount, serverTotal);
        if (pages.length >= desired || value.items.length < PAGE_SIZE || nextOffset + value.items.length >= serverTotal) break;
        nextOffset += PAGE_SIZE;
      }
      if (requestId !== requestNumber.current) return;

      const range = append ? pages : pages.slice(0, Math.min(targetCount, serverTotal));
      setTotal(serverTotal);
      setFacets(pageFacets);
      setItems(current => {
        if (!append) {
          // Pages are concatenated in the API's ARR/name order, so replacing
          // the range also removes stale rows and restores authoritative order.
          loadedCount.current = range.length;
          return range;
        }
        // A page boundary may shift while the run is progressing. Preserve
        // existing positions, update overlapping rows, and append only unseen
        // identities.
        const updates = new Map(range.map(item => [item.account_id, item]));
        const currentIds = new Set<string>();
        const merged = current.map(item => {
          currentIds.add(item.account_id);
          return updates.get(item.account_id) || item;
        });
        const additions = range.filter(item => !currentIds.has(item.account_id));
        const next = [...merged, ...additions];
        loadedCount.current = next.length;
        return next;
      });
    };

    fetchPages().catch(() => {
      if (requestId === requestNumber.current) setFailed(true);
    }).finally(() => {
      if (requestId === requestNumber.current) setLoading(false);
    });
  }, [campaignId, filterParams]);

  const filterKey = [campaignId, filters.status || '', filters.risk || '', filters.chip || '', debouncedQ].join('\u0000');

  // A new filter always starts at page zero. A dashboard refresh instead
  // refreshes page zero in-place, preserving all pages already appended and
  // the user's scroll position. The request token invalidates an in-flight
  // append from the previous filter in either case.
  useEffect(() => {
    const filterChanged = previousFilterKey.current !== filterKey;
    const refreshChanged = previousRefreshKey.current !== refreshKey;
    const retryChanged = previousRetry.current !== retry;
    previousFilterKey.current = filterKey;
    previousRefreshKey.current = refreshKey;
    previousRetry.current = retry;

    if (filterChanged || retryChanged) {
      // Treat the old range as invalid immediately. If a dashboard refresh
      // lands while this reset request is in flight it must not append rows
      // from the previous filter range.
      loadedCount.current = 0;
      setScrollTop(0);
      if (scrollElement.current) scrollElement.current.scrollTop = 0;
      loadPage(0, false);
    } else if (refreshChanged) {
      // Re-fetch the entire loaded range in server order. With no rows loaded
      // yet, still request the first page so newly matching rows appear.
      loadPage(0, false, loadedCount.current);
    }
  }, [filterKey, loadPage, refreshKey, retry]);

  const maybeLoadMore = useCallback(() => {
    if (loading || failed || !items.length || items.length >= total) return;
    loadPage(items.length, true);
  }, [failed, items.length, loadPage, loading, total]);

  // Keep only a viewport-sized slice of the loaded population in the DOM.
  // Spacer rows preserve the native scrollbar while avoiding a layout pass for
  // every account. Additional pages are appended as the viewport nears the end.
  const rowHeight = 70;
  const viewportRows = 10;
  const first = Math.max(0, Math.floor(scrollTop / rowHeight) - 2);
  const last = Math.min(items.length, first + viewportRows + 4);
  const visible = items.slice(first, last);
  const hasFilters = Boolean(filters.status || filters.risk || filters.chip || searchInput);
  const handleScroll = (event: React.UIEvent<HTMLDivElement>) => {
    const element = event.currentTarget;
    setScrollTop(element.scrollTop);
    const nextFirst = Math.max(0, Math.floor(element.scrollTop / rowHeight) - 2);
    if (items.length && nextFirst + viewportRows + 4 >= items.length - 20) maybeLoadMore();
  };

  return <div className="csc-panel">
    <div className="csc-toolbar">
      {chips.map(value => <button className="csc-chip" data-active={filters.chip === value.id} onClick={() => onFiltersChange({...filters, chip: filters.chip === value.id ? undefined : value.id})} key={value.id}>
        {value.label} <span aria-label={`${value.label} count`}>{facets[value.id] ?? '—'}</span>
      </button>)}
      <input aria-label="Search accounts" placeholder="Search" value={searchInput} onChange={event => {
        const value = event.target.value;
        setSearchInput(value);
        onFiltersChange({...filters, q: value || undefined});
      }} />
      {hasFilters && <button className="csc-chip" onClick={() => {setSearchInput(''); onFiltersChange({})}}>Clear filters</button>}
    </div>
    <div className="csc-table-summary">Showing {items.length.toLocaleString()} of {total.toLocaleString()}</div>
    {loading && <div className="csc-progress" role="progressbar" aria-label="Loading accounts"><span /></div>}
    {failed ? <div className="csc-empty">Couldn't load accounts. <button className="csc-chip" onClick={() => setRetry(value => value + 1)}>Retry</button></div> : <>
      <div className="csc-table-scroll" ref={scrollElement} onScroll={handleScroll}>
        <table className="csc-table"><thead><tr><th>Account</th><th>ARR</th><th>Risk</th><th>v1 usage</th><th>v2 adoption</th><th>Owner</th><th>Status</th><th>Blocker</th></tr></thead>
          <tbody>
            {first > 0 && <tr aria-hidden="true"><td colSpan={8} style={{height:first * rowHeight, padding:0}} /></tr>}
            {visible.map(account => <tr className="csc-row" style={{height:rowHeight}} onClick={() => onSelect(account)} key={account.account_id}>
              <td><strong>{account.account_name}</strong><br/><small>{account.account_id}</small></td>
              <td>${account.arr.toLocaleString()}</td><td>{account.risk}</td>
              <td>{account.legacy_usage.toLocaleString()}</td><td>{account.replacement_usage.toLocaleString()}</td>
              <td>{account.owner}</td><td><span className={`csc-pill csc-${account.status}`}>{account.status.replaceAll('_',' ')}</span></td>
              <td>{account.blocker_type || '—'}</td>
            </tr>)}
            {last < items.length && <tr aria-hidden="true"><td colSpan={8} style={{height:(items.length - last) * rowHeight, padding:0}} /></tr>}
          </tbody>
        </table>
      </div>
      {!items.length && <div className="csc-empty">No accounts match the current filters.</div>}
    </>}
  </div>;
}
