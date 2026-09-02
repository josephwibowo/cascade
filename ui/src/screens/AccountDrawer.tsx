import {useEffect, useState} from 'react';
import {api, type Account, type PendingException} from '../api/client';
import ViewOrchestration from '../components/ViewOrchestration';

type TimelineEntry = {id: number; event_type: string; timestamp: string; summary: string; source: string};

export default function AccountDrawer({campaignId, accountId, onClose, onOpenExceptions}: {campaignId: string; accountId: string; onClose: () => void; onOpenExceptions: () => void}) {
  const [account, setAccount] = useState<Account | null>(null);
  const [accountLoading, setAccountLoading] = useState(true);
  const [timeline, setTimeline] = useState<TimelineEntry[]>([]);
  const [exception, setException] = useState<PendingException | null>(null);

  useEffect(() => {
    let active = true;
    setAccount(null);
    setAccountLoading(true);
    api.account(accountId, campaignId).then(value => {if (active) setAccount(value)}).catch(() => {if (active) setAccount(null)}).finally(() => {if (active) setAccountLoading(false)});
    api.timeline(campaignId, accountId).then(value => {if (active) setTimeline(value)}).catch(() => {if (active) setTimeline([])});
    api.exceptions().then(value => {if (active) setException(value.items.find(item => item.account_id === accountId && item.campaign_id === campaignId) || null)}).catch(() => {if (active) setException(null)});
    return () => {active = false};
  }, [accountId, campaignId]);

  // Hoisted so the trend bars don't recompute it inside both .map() callbacks
  // (once per day, per series) on every render.
  const peak = account ? Math.max(...account.daily_v1, ...account.daily_v2, 1) : 1;

  return <aside className="csc-drawer">
    <button className="csc-close" onClick={onClose}>×</button>
    {!account ? <div className="csc-empty">{accountLoading ? 'Loading account…' : 'Account not available.'}</div> : <>
      <div className="csc-eyebrow">Account migration</div><h2>{account.account_name}</h2>
      <div className="csc-subtitle">{account.tier} · {account.region} · {account.owner}</div>
      <p><strong>${account.arr.toLocaleString()}</strong> ARR</p>
      <div className="csc-toolbar"><span className={`csc-pill csc-${account.status}`}>{account.status}</span><ViewOrchestration task={account.latest_airflow_task_instance} /></div>
      <div className="csc-card"><div className="csc-label">Telemetry trend · v1 and v2</div>
        <div className="csc-trend">{account.daily_v1.map((value,index) => <i key={`v1-${index}`} style={{height:`${Math.max(3, value / peak * 80)}px`}} />)}{account.daily_v2.map((value,index) => <i key={`v2-${index}`} style={{height:`${Math.max(3, value / peak * 80)}px`,background:'#5bc59a'}} />)}</div>
        <div className="csc-subtitle">Trailing seven days · legacy {account.legacy_usage.toLocaleString()} · replacement {account.replacement_usage.toLocaleString()}</div>
      </div>
      {account.blocker_type && <div className="csc-note">Blocker: {account.blocker_type}. Contract evidence: {String(account.evidence?.commitment_expiry || 'none')}.</div>}
      {account.brief && <div className="csc-card"><div className="csc-label">Migration brief {account.brief_source === 'llm' && <span className="csc-pill csc-MIGRATED">AI generated</span>}</div><p>{String(account.brief.summary || '')}</p><strong>Next step:</strong> {String(account.brief.proposed_next_step || '')}</div>}
      <h3>Timeline</h3><div className="csc-timeline">{timeline.map(event => <div className="csc-event" key={event.id}><time>{new Date(event.timestamp).toLocaleString()}</time><div>{event.summary}</div><small>{event.source}</small></div>)}</div>
      {exception && <button className="csc-action" data-primary="true" onClick={onOpenExceptions}>Review decision</button>}
    </>}
  </aside>;
}
