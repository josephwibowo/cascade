import {useEffect, useState} from 'react';
import {api} from '../api/client';

export default function LiveRail({campaignId}: {campaignId: string}) {
  const [data, setData] = useState<Record<string, unknown> | null>(null);
  useEffect(() => { let active = true; const poll = () => api.orchestration(campaignId).then(value => active && setData(value)).catch(() => active && setData(null)); poll(); const timer = window.setInterval(poll, 2200); return () => {active = false; window.clearInterval(timer)}; }, [campaignId]);
  const states = (data?.states || {}) as Record<string, number>;
  return <aside className="csc-panel csc-rail"><h2>Airflow orchestration</h2>{data ? <><div className="csc-note">Counters below are task instances reported by Airflow.</div><div className="csc-railline"><span>Mapped tasks</span><strong>{String(data.mapped ?? 0)}</strong></div><div className="csc-railline"><span>Running</span><strong>{states.running ?? 0}</strong></div><div className="csc-railline"><span>Success</span><strong>{states.success ?? 0}</strong></div><div className="csc-railline"><span>Awaiting input</span><strong>{states.awaiting_input ?? 0}</strong></div></> : <div className="csc-empty">Airflow state unavailable</div>}</aside>;
}
