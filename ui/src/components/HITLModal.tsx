import {useState} from 'react';
import {api} from '../api/client';
import ViewOrchestration from './ViewOrchestration';
import type {AirflowTask} from '../api/client';

const OPTIONS = ['Grant extension to November 15', 'Keep October 31 deadline', 'Escalate for legal review'];
export default function HITLModal({exceptionId, task, onClose, onDone}: {exceptionId: string; task?: AirflowTask | null; onClose: () => void; onDone: () => void}) {
  const [option, setOption] = useState(OPTIONS[0]); const [reason, setReason] = useState(''); const [error, setError] = useState(''); const [busy, setBusy] = useState(false);
  const submit = () => {setBusy(true); api.respond(exceptionId, [option], reason).then(() => {onDone(); onClose()}).catch(e => setError(String(e))).finally(() => setBusy(false))};
  return <div className="csc-modal"><div><button className="csc-close" onClick={onClose}>×</button><div className="csc-eyebrow">Airflow human input</div><div className="csc-toolbar"><h2>Resolve migration exception</h2><ViewOrchestration task={task} /></div>{OPTIONS.map(value => <button className="csc-chip csc-option" data-active={value === option} onClick={() => setOption(value)} key={value}>{value}</button>)}<label className="csc-label">Reason (required)<textarea value={reason} onChange={event => setReason(event.target.value)} /></label>{error && <div className="csc-note">{error}</div>}<button className="csc-action" data-primary="true" disabled={busy || !reason.trim()} onClick={submit}>{busy ? 'Waiting for Airflow…' : 'Submit decision'}</button></div></div>;
}
