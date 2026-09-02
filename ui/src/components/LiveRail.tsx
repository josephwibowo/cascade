import {useEffect, useRef, useState} from 'react';
import {api, type AirflowTask} from '../api/client';

export default function LiveRail({campaignId, refreshKey, onRun}: {campaignId: string; refreshKey?: number; onRun?: (task: AirflowTask) => void}) {
  const [data, setData] = useState<Awaited<ReturnType<typeof api.orchestration>> | null>(null);
  // The rail is already the one poller of the orchestration endpoint, so it
  // reports the run it finds rather than letting anything else poll for the
  // same answer. Held in a ref so a new callback identity per parent render
  // cannot tear the poller down and restart it.
  const report = useRef(onRun);
  report.current = onRun;
  const reportedRun = useRef<string | null>(null);
  useEffect(() => {
    let active = true;
    let timer: number | undefined;
    const terminal = new Set(['success', 'failed', 'upstream_failed', 'skipped', 'removed', 'cancelled', 'canceled']);
    // A terminal assessment must not own the rail forever. Dashboard bumps
    // refreshKey when a scenario action triggers a new run, which tears down
    // this poller and starts a fresh one against whichever run the
    // orchestration endpoint now reports.
    setData(null);
    const stop = () => {active = false; if (timer !== undefined) window.clearTimeout(timer)};
    // Chain each poll off the previous answer rather than firing on a fixed
    // interval. A slow response would otherwise queue requests until they
    // exhaust the browser's per-origin connections and starve the rest of
    // the dashboard.
    const again = () => {if (active) timer = window.setTimeout(poll, 2200)};
    const poll = () => api.orchestration(campaignId).then(value => {
      if (!active) return;
      setData(value);
      if (value.dag_id && value.run_id && value.run_id !== reportedRun.current) {
        reportedRun.current = value.run_id;
        report.current?.({dag_id: value.dag_id, run_id: value.run_id, task_id: value.task_id || '', map_index: -1});
      }
      const state = String(value.dag_run_state ?? value.run_state ?? value.state ?? '').toLowerCase();
      if (terminal.has(state)) return stop();
      again();
    }).catch(() => {
      if (!active) return;
      setData(null);
      again();
    });
    poll();
    return stop;
  }, [campaignId, refreshKey]);
  const states = data?.states || {};
  const runState = data?.dag_run_state ?? data?.run_state ?? data?.state;
  return <aside className="csc-panel csc-rail"><h2>Airflow orchestration</h2>{data ? <><div className="csc-note">Counters below are task instances reported by Airflow.</div><div className="csc-railline"><span>Run state</span><strong>{String(runState ?? '—')}</strong></div><div className="csc-railline"><span>Mapped tasks</span><strong>{states.mapped ?? data.mapped ?? '—'}</strong></div><div className="csc-railline"><span>Running</span><strong>{states.running ?? '—'}</strong></div><div className="csc-railline"><span>Success</span><strong>{states.success ?? '—'}</strong></div><div className="csc-railline"><span>Failed</span><strong>{states.failed ?? '—'}</strong></div><div className="csc-railline"><span>Awaiting input</span><strong>{states.awaiting_input ?? '—'}</strong></div></> : <div className="csc-empty">Airflow state unavailable</div>}</aside>;
}
