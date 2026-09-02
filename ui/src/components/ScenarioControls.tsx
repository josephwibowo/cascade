import {useEffect, useState} from 'react';
import {api} from '../api/client';

const TERMINAL_STATES = new Set(['success', 'failed', 'upstream_failed', 'skipped', 'removed', 'cancelled', 'canceled']);

export default function ScenarioControls({campaignId, verificationRunId, onSuccess}: {campaignId: string; verificationRunId?: string | null; onSuccess: () => Promise<unknown> | void}) {
  const [snapshot, setSnapshot] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [triggeredRunId, setTriggeredRunId] = useState<string | null>(null);
  const [verificationState, setVerificationState] = useState<string | null>(null);
  const [error, setError] = useState('');

  // Prefer a run returned by a just-completed action until Dashboard's
  // campaign refresh delivers the same persisted identity. This closes the
  // small window in which a successful trigger could otherwise be clicked a
  // second time, while the campaign prop also makes the state reload-safe.
  const currentRunId = triggeredRunId || verificationRunId || null;

  useEffect(() => {
    api.scenario().then(value => setSnapshot(value.snapshot)).catch(reason => setError(String(reason)));
  }, []);

  useEffect(() => {
    let active = true;
    let timer: number | undefined;
    if (!currentRunId) {
      setVerificationState(null);
      return () => undefined;
    }
    setVerificationState(null);
    const stop = () => {active = false; if (timer !== undefined) window.clearTimeout(timer)};
    const again = () => {if (active) timer = window.setTimeout(poll, 2200)};
    const poll = () => api.orchestration(campaignId).then(value => {
      if (!active) return;
      // The endpoint reads the same persisted current run that Dashboard does.
      // If a parent refresh raced the trigger, follow the newer identity.
      if (value.run_id && value.run_id !== currentRunId) {
        setTriggeredRunId(value.run_id);
        return;
      }
      const state = String(value.dag_run_state ?? value.run_state ?? value.state ?? '').toLowerCase();
      setVerificationState(state);
      if (!TERMINAL_STATES.has(state)) again();
    }).catch(() => {
      if (!active) return;
      // An active run stays guarded when Airflow is briefly unavailable; a
      // retry will establish its terminal state before re-enabling the button.
      setVerificationState(null);
      again();
    });
    poll();
    return stop;
  }, [campaignId, currentRunId]);

  // Until the current run has been observed as terminal, keep controls
  // disabled. This derives from Airflow's DAG run rather than campaign
  // lifecycle status (the day-7 wave intentionally leaves that IN_PROGRESS).
  const verificationActive = Boolean(currentRunId && !TERMINAL_STATES.has(verificationState || ''));

  const advance = async () => {
    if (!snapshot || busy || verificationActive) return;
    setBusy(true);
    setError('');
    try {
      if (snapshot === 'day0') {
        const result = await api.advance('day7');
        setSnapshot(result.snapshot);
        setVerificationState(null);
        setTriggeredRunId(result.dag_run_id || result.run_id || null);
      } else {
        const result = await api.verify(campaignId);
        setVerificationState(null);
        setTriggeredRunId(result.dag_run_id || result.run_id || null);
      }
      await onSuccess();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  };

  return <div className="csc-scenario"><span className="csc-subtitle">Scenario: {snapshot || '—'}</span>{snapshot && <button className="csc-action" data-primary="true" disabled={busy || verificationActive} onClick={advance}>{busy ? 'Working…' : snapshot === 'day0' ? 'Advance to day 7' : 'Run verification'}</button>}{error && <span className="csc-scenario-error">{error}</span>}</div>;
}
