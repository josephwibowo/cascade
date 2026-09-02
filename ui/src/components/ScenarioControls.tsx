import {useEffect, useState} from 'react';
import {api} from '../api/client';

const TERMINAL_STATES = new Set(['success', 'failed', 'upstream_failed', 'skipped', 'removed', 'cancelled', 'canceled']);

export default function ScenarioControls({campaignId, onSuccess}: {campaignId: string; onSuccess: () => Promise<unknown> | void}) {
  const [snapshot, setSnapshot] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // Until the current verification run has been observed as terminal, keep the
  // control disabled. This derives from Airflow's DAG run rather than campaign
  // lifecycle status (the day-7 wave intentionally leaves that IN_PROGRESS).
  const [verificationActive, setVerificationActive] = useState(false);
  const [pollKey, setPollKey] = useState(0);
  const [error, setError] = useState('');

  useEffect(() => {
    api.scenario().then(value => setSnapshot(value.snapshot)).catch(reason => setError(String(reason)));
  }, []);

  useEffect(() => {
    let active = true;
    let timer: number | undefined;
    const stop = () => {active = false; if (timer !== undefined) window.clearTimeout(timer)};
    const again = () => {if (active) timer = window.setTimeout(poll, 2200)};
    const poll = () => api.orchestration(campaignId).then(value => {
      if (!active) return;
      // The endpoint reports whichever run the campaign currently points at, so
      // a migration_verification dag_id is the run this control owns. Reading it
      // per poll — rather than from a prop — keeps a mid-run reload guarded
      // without widening the campaign rollup's response contract.
      const state = String(value.dag_run_state ?? value.run_state ?? value.state ?? '').toLowerCase();
      const running = value.dag_id === 'migration_verification' && !TERMINAL_STATES.has(state);
      setVerificationActive(running);
      // Nothing else in this screen starts a run, so stop chaining once the run
      // settles; a trigger below restarts the poller through pollKey.
      if (running) again();
    }).catch(() => {
      // Airflow being briefly unavailable must not unlock the button under an
      // active run; retry until a poll establishes the real state.
      if (active) again();
    });
    poll();
    return stop;
  }, [campaignId, pollKey]);

  const advance = async () => {
    if (!snapshot || busy || verificationActive) return;
    setBusy(true);
    setError('');
    try {
      if (snapshot === 'day0') {
        const result = await api.advance('day7');
        setSnapshot(result.snapshot);
      } else {
        await api.verify(campaignId);
      }
      // Both routes persist the new run before answering, so guard the control
      // immediately and let the restarted poller take over from here.
      setVerificationActive(true);
      setPollKey(value => value + 1);
      await onSuccess();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusy(false);
    }
  };

  return <div className="csc-scenario"><span className="csc-subtitle">Scenario: {snapshot || '—'}</span>{snapshot && <button className="csc-action" data-primary="true" disabled={busy || verificationActive} onClick={advance}>{busy ? 'Working…' : snapshot === 'day0' ? 'Advance to day 7' : 'Run verification'}</button>}{error && <span className="csc-scenario-error">{error}</span>}</div>;
}
