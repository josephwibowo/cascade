import {useCallback, useEffect, useState} from 'react';
import {api, type PendingException} from '../api/client';
import HITLModal from '../components/HITLModal';

function awaiting(exception: PendingException) {
  return exception.hitl_details?.state === 'awaiting_input';
}

export default function ExceptionQueue({campaignId, onClose, onUpdated}: {campaignId: string; onClose: () => void; onUpdated: () => void}) {
  const [exceptions, setExceptions] = useState<PendingException[]>([]);
  const [airflowError, setAirflowError] = useState<string | null>(null);
  const [selected, setSelected] = useState<PendingException | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(() => {
    setLoading(true);
    api.exceptions().then(value => {
      const rows = value.items.filter(item => item.campaign_id === campaignId).sort((a, b) => Number(awaiting(b)) - Number(awaiting(a)));
      setExceptions(rows);
      setAirflowError(value.airflow_error || null);
    }).catch(error => setAirflowError(String(error))).finally(() => setLoading(false));
  }, [campaignId]);

  useEffect(() => {refresh()}, [refresh]);

  const done = () => {
    setSelected(null);
    refresh();
    onUpdated();
  };

  return <aside className="csc-drawer csc-queue">
    <button className="csc-close" onClick={onClose}>×</button>
    <div className="csc-eyebrow">Pending Airflow decisions</div><h2>Exception queue</h2>
    {airflowError && <div className="csc-note csc-error">Airflow error: {airflowError}</div>}
    {loading ? <div className="csc-empty">Loading exceptions…</div> : !exceptions.length ? <div className="csc-empty">No pending exceptions.</div> : <div className="csc-exceptions">
      {exceptions.map(exception => <div className="csc-exception" key={exception.id}>
        <div><strong>{exception.account_name || exception.account_id}</strong><div className="csc-subtitle">{exception.exception_type} · {exception.arr == null ? 'ARR unavailable' : `$${exception.arr.toLocaleString()}`}</div></div>
        <span className="csc-pill csc-exception-state">{awaiting(exception) ? 'Awaiting input' : 'Pending'}</span>
        {exception.hitl_task_id ? <button className="csc-action" data-primary="true" onClick={() => setSelected(exception)}>Decide</button> : <span className="csc-subtitle">Informational</span>}
      </div>)}
    </div>}
    {selected && <HITLModal exceptionId={selected.id} task={selected.airflow_dag_run_id && selected.hitl_task_id ? {dag_id:'exception_resolution', run_id:selected.airflow_dag_run_id, task_id:selected.hitl_task_id, map_index:-1} : null} details={selected.hitl_details} onClose={() => setSelected(null)} onDone={done} />}
  </aside>;
}
