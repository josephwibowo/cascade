import {useEffect, useState} from 'react';
import {api, type AirflowHitlDetails, type AirflowTask} from '../api/client';
import ViewOrchestration from './ViewOrchestration';

type ParameterValue = {title?: string; description?: string; value?: unknown; type?: string; minLength?: number} | string | number | boolean | null;

function parameterText(value: ParameterValue): string {
  if (value && typeof value === 'object' && 'value' in value) return value.value == null ? '' : String(value.value);
  return value == null ? '' : String(value);
}

export default function HITLModal({exceptionId, task, details, onClose, onDone}: {exceptionId: string; task?: AirflowTask | null; details?: AirflowHitlDetails | null; onClose: () => void; onDone: () => void}) {
  const options = details?.options || [];
  const parameters = Object.entries(details?.parameters || {}) as [string, ParameterValue][];
  const [option, setOption] = useState('');
  const [parameterValues, setParameterValues] = useState<Record<string, string>>({});
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    setOption(options[0] || '');
    setParameterValues(Object.fromEntries(parameters.map(([name, value]) => [name, parameterText(value)])));
  }, [details]);

  const reason = parameterValues.reason || '';
  const submit = () => {
    setBusy(true);
    api.respond(exceptionId, [option], reason, parameterValues).then(() => {onDone(); onClose()}).catch(e => setError(String(e))).finally(() => setBusy(false));
  };
  return <div className="csc-modal"><div><button className="csc-close" onClick={onClose}>×</button><div className="csc-eyebrow">Airflow human input</div><div className="csc-toolbar"><h2>{details?.subject || 'Human decision required'}</h2><ViewOrchestration task={task} /></div>{details?.body && <div className="csc-note">{details.body}</div>}{options.map(value => <button className="csc-chip csc-option" data-active={value === option} onClick={() => setOption(value)} key={value}>{value}</button>)}{parameters.map(([name, value]) => <label className="csc-label" key={name}>{(value && typeof value === 'object' && 'title' in value && value.title) || name}{value && typeof value === 'object' && 'description' in value && value.description && <small> · {value.description}</small>}<textarea value={parameterValues[name] || ''} onChange={event => setParameterValues(current => ({...current, [name]: event.target.value}))} /></label>)}{!details && <div className="csc-note">Airflow decision details unavailable.</div>}{error && <div className="csc-note">{error}</div>}<button className="csc-action" data-primary="true" disabled={busy || !option || !reason.trim()} onClick={submit}>{busy ? 'Waiting for Airflow…' : 'Submit decision'}</button></div></div>;
}
