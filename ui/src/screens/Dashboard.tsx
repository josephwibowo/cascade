import {useCallback, useEffect, useRef, useState} from 'react';
import {api, type Campaign} from '../api/client';
import AccountTable, {type Filters} from './AccountTable';
import AccountDrawer from './AccountDrawer';
import ExceptionQueue from './ExceptionQueue';
import LiveRail from '../components/LiveRail';
import ScenarioControls from '../components/ScenarioControls';
import ViewOrchestration from '../components/ViewOrchestration';

const TERMINAL_STATES = new Set(['MIGRATED', 'FAILED', 'CANCELLED', 'CANCELED', 'SUCCESS']);

function Metric({label, value, onClick}: {label: string; value: string | number; onClick?: () => void}) {
  const content = <><div className="csc-label">{label}</div><div className="csc-value">{typeof value === 'number' ? value.toLocaleString() : value}</div></>;
  return onClick ? <button className="csc-card" onClick={onClick} style={{textAlign:'left',cursor:'pointer'}}>{content}</button> : <div className="csc-card" style={{textAlign:'left'}}>{content}</div>;
}

function filterLabel(key: string, value: string) {
  const title = key === 'q' ? 'Search' : key[0].toUpperCase() + key.slice(1);
  return `${title}: ${value.replaceAll('_', ' ')}`;
}

export default function Dashboard({campaignId}: {campaignId: string}) {
  const [campaign, setCampaign] = useState<Campaign | null>(null);
  const campaignRef = useRef<Campaign | null>(null);
  const [dataRevision, setDataRevision] = useState(0);
  const [error, setError] = useState('');
  const [selected, setSelected] = useState<{account_id: string; campaign_id: string} | null>(null);
  const [filters, setFilters] = useState<Filters>({});
  const [exceptionCount, setExceptionCount] = useState<number | null>(null);
  const [awaiting, setAwaiting] = useState<number | null>(null);
  const [showQueue, setShowQueue] = useState(false);

  const load = useCallback(async () => {
    const [campaignResult, exceptionsResult] = await Promise.allSettled([api.campaign(campaignId), api.exceptions()]);
    let latest = campaignRef.current;
    if (campaignResult.status === 'fulfilled') {
      latest = campaignResult.value;
      campaignRef.current = latest;
      setCampaign(latest);
      setDataRevision(value => value + 1);
      setError('');
    } else if (!latest) {
      setError(String(campaignResult.reason));
    }
    if (exceptionsResult.status === 'fulfilled') {
      setExceptionCount(exceptionsResult.value.product_exception_count);
      setAwaiting(exceptionsResult.value.airflow_awaiting_input);
    }
    return latest;
  }, [campaignId]);

  useEffect(() => {
    let active = true;
    let timer: number | undefined;
    const poll = () => {
      load().finally(() => {
        if (!active) return;
        const status = String(campaignRef.current?.status || '').toUpperCase();
        if (!TERMINAL_STATES.has(status)) timer = window.setTimeout(poll, 4000);
      });
    };
    poll();
    return () => {active = false; if (timer !== undefined) window.clearTimeout(timer)};
  }, [campaignId, load]);

  if (!campaign) return <div className="csc-shell csc-empty"><h1>Cascade</h1><p>{error ? 'The campaign is not ready yet.' : 'Loading campaign state…'}</p></div>;

  const statuses = campaign.status_distribution;
  const risks = campaign.risk_distribution;
  const total = Math.max(campaign.affected_accounts, 1);
  const days = Math.max(0, Math.ceil((new Date(campaign.deadline).getTime() - Date.now()) / 86400000));
  const clearFilters = () => setFilters({});
  const chooseStatus = (status: string) => setFilters(current => ({...current, status: current.status === status ? undefined : status}));
  const chooseRisk = (risk: string) => setFilters(current => ({...current, risk: current.risk === risk ? undefined : risk}));
  const statusLabel = (status: string) => status.replaceAll('_',' ');
  const activeFilters = Object.entries(filters).filter(([, value]) => value);

  return <div className="csc-shell">
    <header className="csc-header"><div><div className="csc-eyebrow">Product change control plane</div><h1 className="csc-title">{campaign.name}</h1><div className="csc-subtitle">{campaign.change_type} · deadline {campaign.deadline}</div></div><div className="csc-header-actions"><ScenarioControls campaignId={campaignId} verificationRunId={campaign.verification_run_id} onSuccess={load} /><ViewOrchestration task={campaign.airflow_dag_run_id ? {dag_id:'product_change_assessment',run_id:campaign.airflow_dag_run_id,task_id:'assess_account',map_index:-1} : null} /></div></header>
    <div className="csc-grid">
      <Metric label="Affected accounts" value={campaign.affected_accounts} onClick={clearFilters} />
      <Metric label="Affected ARR" value={`$${campaign.affected_arr.toLocaleString()}`} onClick={clearFilters} />
      <Metric label="Days to sunset" value={days} />
      <Metric label="Migration completion" value={`${(campaign.migration_completion * 100).toFixed(1)}%`} onClick={() => chooseStatus('MIGRATED')} />
      <Metric label="Blocked accounts" value={campaign.blocked_accounts} onClick={() => chooseStatus('BLOCKED')} />
      <Metric label="Exceptions" value={exceptionCount == null ? '—' : `${exceptionCount} · ${awaiting == null ? 'Airflow unavailable' : `${awaiting} awaiting input`}`} onClick={() => setShowQueue(true)} />
    </div>
    <div className="csc-panel"><h2>Lifecycle distribution</h2><div className="csc-bar">{Object.entries(statuses).map(([status,count]) => <button aria-label={`${statusLabel(status)} ${count}`} key={status} className={`csc-${status}`} style={{width:`${count / total * 100}%`}} onClick={() => chooseStatus(status)} />)}</div><div className="csc-subtitle csc-distribution-legend">{Object.entries(statuses).map(([status,count]) => <button className="csc-link" data-active={filters.status === status} onClick={() => chooseStatus(status)} key={status}>{statusLabel(status)} {count.toLocaleString()}</button>)}</div></div>
    <div className="csc-panel"><h2>Risk distribution</h2><div className="csc-bar">{Object.entries(risks).map(([risk,count]) => <button aria-label={`${risk} ${count}`} key={risk} className={`csc-risk-${risk}`} style={{width:`${count / total * 100}%`}} onClick={() => chooseRisk(risk)} />)}</div><div className="csc-subtitle csc-distribution-legend">{Object.entries(risks).map(([risk,count]) => <button className="csc-link" data-active={filters.risk === risk} onClick={() => chooseRisk(risk)} key={risk}>{statusLabel(risk)} {count.toLocaleString()}</button>)}</div></div>
    <div className="csc-layout"><main><h2>Blast radius</h2>{activeFilters.length > 0 && <div className="csc-filter-bar" aria-label="Active filters">{activeFilters.map(([key, value]) => <button className="csc-filter-token" key={key} onClick={() => setFilters(current => ({...current, [key]: undefined}))}>{filterLabel(key, String(value))} ×</button>)}<button className="csc-link" onClick={clearFilters}>Clear all</button></div>}<AccountTable campaignId={campaignId} refreshKey={dataRevision} filters={filters} onFiltersChange={setFilters} onSelect={account => setSelected({account_id: account.account_id, campaign_id: account.campaign_id})} /></main><LiveRail campaignId={campaignId} runId={campaign.verification_run_id || campaign.airflow_dag_run_id} /></div>
    {selected && <AccountDrawer campaignId={selected.campaign_id} accountId={selected.account_id} onClose={() => setSelected(null)} onOpenExceptions={() => setShowQueue(true)} />}
    {showQueue && <ExceptionQueue campaignId={campaignId} onClose={() => setShowQueue(false)} onUpdated={() => {void load()}} />}
  </div>;
}
