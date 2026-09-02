import {useEffect, useState} from 'react';
import {api} from '../api/client';

export default function LiveRail({campaignId}: {campaignId: string}) {
  const [data, setData] = useState<Awaited<ReturnType<typeof api.orchestration>> | null>(null);
  useEffect(() => {
    let active = true;
    let timer: number | undefined;
    const terminal = new Set(['success', 'failed', 'upstream_failed', 'skipped', 'removed', 'cancelled', 'canceled']);
    const stop = () => {if (timer !== undefined) window.clearInterval(timer)};
    const poll = () => api.orchestration(campaignId).then(value => {
      if (!active) return;
      setData(value);
      const state = String(value.dag_run_state ?? value.run_state ?? value.state ?? '').toLowerCase();
      if (terminal.has(state)) stop();
    }).catch(() => active && setData(null));
    timer = window.setInterval(poll, 2200);
    poll();
    return () => {active = false; stop()};
  }, [campaignId]);
  const states = data?.states || {};
  const runState = data?.dag_run_state ?? data?.run_state ?? data?.state;
  return <aside className="csc-panel csc-rail"><h2>Airflow orchestration</h2>{data ? <><div className="csc-note">Counters below are task instances reported by Airflow.</div><div className="csc-railline"><span>Run state</span><strong>{String(runState ?? '—')}</strong></div><div className="csc-railline"><span>Mapped tasks</span><strong>{states.mapped ?? data.mapped ?? '—'}</strong></div><div className="csc-railline"><span>Running</span><strong>{states.running ?? '—'}</strong></div><div className="csc-railline"><span>Success</span><strong>{states.success ?? '—'}</strong></div><div className="csc-railline"><span>Awaiting input</span><strong>{states.awaiting_input ?? '—'}</strong></div></> : <div className="csc-empty">Airflow state unavailable</div>}</aside>;
}
