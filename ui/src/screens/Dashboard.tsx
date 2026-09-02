import {useEffect, useState} from 'react';
import {api, type Account, type Campaign} from '../api/client';
import AccountTable, {type Chip} from './AccountTable';
import AccountDrawer from './AccountDrawer';
import LiveRail from '../components/LiveRail';
import ViewOrchestration from '../components/ViewOrchestration';

function Metric({label, value, onClick}: {label: string; value: string | number; onClick?: () => void}) {
  return <button className="csc-card" onClick={onClick} style={{textAlign:'left',cursor:onClick?'pointer':'default'}}><div className="csc-label">{label}</div><div className="csc-value">{typeof value === 'number' ? value.toLocaleString() : value}</div></button>;
}

export default function Dashboard({campaignId}: {campaignId: string}) {
  const [campaign, setCampaign] = useState<Campaign | null>(null);
  const [error, setError] = useState('');
  const [selected, setSelected] = useState<Account | null>(null);
  const [statusFilter, setStatusFilter] = useState<string | undefined>();
  const [riskFilter, setRiskFilter] = useState<string | undefined>();
  const [chipFilter, setChipFilter] = useState<Chip | ''>('');
  const [filterReset, setFilterReset] = useState(0);
  const [exceptionCount, setExceptionCount] = useState<number | null>(null);
  const [awaiting, setAwaiting] = useState<number | null>(null);

  const load = () => {
    api.campaign(campaignId).then(setCampaign).catch(e => {setCampaign(null);setError(String(e))});
    api.exceptions().then(value => {setExceptionCount(value.product_exception_count);setAwaiting(value.airflow_awaiting_input)}).catch(() => {});
  };
  useEffect(() => {load()}, [campaignId]);
  if (!campaign) return <div className="csc-shell csc-empty"><h1>Cascade</h1><p>{error ? 'The campaign is not ready yet.' : 'Loading campaign state…'}</p></div>;

  const statuses = campaign.status_distribution;
  const risks = campaign.risk_distribution;
  const total = Math.max(campaign.affected_accounts, 1);
  const days = Math.max(0, Math.ceil((new Date(campaign.deadline).getTime() - Date.now()) / 86400000));
  const clearFilters = () => {setStatusFilter(undefined);setRiskFilter(undefined);setChipFilter('');setFilterReset(value => value + 1)};
  const chooseStatus = (status: string) => {setStatusFilter(statusFilter === status ? undefined : status);setRiskFilter(undefined);setChipFilter('');setFilterReset(value => value + 1)};
  const chooseRisk = (risk: string) => {setRiskFilter(riskFilter === risk ? undefined : risk);setStatusFilter(undefined);setChipFilter('');setFilterReset(value => value + 1)};
  const statusLabel = (status: string) => status.replaceAll('_',' ');

  return <div className="csc-shell">
    <header className="csc-header"><div><div className="csc-eyebrow">Product change control plane</div><h1 className="csc-title">{campaign.name}</h1><div className="csc-subtitle">{campaign.change_type} · deadline {campaign.deadline}</div></div><ViewOrchestration task={campaign.airflow_dag_run_id ? {dag_id:'product_change_assessment',run_id:campaign.airflow_dag_run_id,task_id:'assess_account',map_index:-1} : null} /></header>
    <div className="csc-grid">
      <Metric label="Affected accounts" value={campaign.affected_accounts} onClick={clearFilters} />
      <Metric label="Affected ARR" value={`$${campaign.affected_arr.toLocaleString()}`} onClick={clearFilters} />
      <Metric label="Days to sunset" value={days} />
      <Metric label="Migration completion" value={`${(campaign.migration_completion * 100).toFixed(1)}%`} onClick={() => chooseStatus('MIGRATED')} />
      <Metric label="Blocked accounts" value={campaign.blocked_accounts} onClick={() => chooseStatus('BLOCKED')} />
      <Metric label="Exceptions" value={exceptionCount == null ? '—' : `${exceptionCount} · ${awaiting == null ? 'Airflow unavailable' : `${awaiting} awaiting input`}`} />
    </div>
    <div className="csc-panel">
      <h2>Lifecycle distribution</h2>
      <div className="csc-bar">{Object.entries(statuses).map(([status,count]) => <button aria-label={`${statusLabel(status)} ${count}`} key={status} className={`csc-${status}`} style={{width:`${count / total * 100}%`}} onClick={() => chooseStatus(status)} />)}</div>
      <div className="csc-subtitle csc-distribution-legend">{Object.entries(statuses).map(([status,count]) => <button className="csc-link" data-active={statusFilter === status} onClick={() => chooseStatus(status)} key={status}>{statusLabel(status)} {count.toLocaleString()}</button>)}</div>
    </div>
    <div className="csc-panel">
      <h2>Risk distribution</h2>
      <div className="csc-bar">{Object.entries(risks).map(([risk,count]) => <button aria-label={`${risk} ${count}`} key={risk} className={`csc-risk-${risk}`} style={{width:`${count / total * 100}%`}} onClick={() => chooseRisk(risk)} />)}</div>
      <div className="csc-subtitle csc-distribution-legend">{Object.entries(risks).map(([risk,count]) => <button className="csc-link" data-active={riskFilter === risk} onClick={() => chooseRisk(risk)} key={risk}>{statusLabel(risk)} {count.toLocaleString()}</button>)}</div>
    </div>
    <div className="csc-layout"><main><h2>Blast radius</h2><AccountTable campaignId={campaignId} statusFilter={statusFilter} riskFilter={riskFilter} chipFilter={chipFilter} chipCounts={campaign.chip_counts} clearToken={filterReset} onChipChange={value => {setChipFilter(value);setStatusFilter(undefined);setRiskFilter(undefined)}} onClearFilters={clearFilters} onSelect={setSelected} /></main><LiveRail campaignId={campaignId} /></div>
    {selected && <AccountDrawer account={selected} onClose={() => setSelected(null)} />}
  </div>;
}
