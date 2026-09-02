import type {AirflowTask} from '../api/client';

export function orchestrationUrl(task?: AirflowTask | null) {
  if (!task) return '/dags';
  const map = task.map_index >= 0 ? `&map_index=${task.map_index}` : '';
  return `/dags/${encodeURIComponent(task.dag_id)}/runs/${encodeURIComponent(task.run_id)}/grid?task_id=${encodeURIComponent(task.task_id)}${map}`;
}

export default function ViewOrchestration({task, label = 'View Orchestration'}: {task?: AirflowTask | null; label?: string}) {
  return <a className="csc-action" data-primary="true" href={orchestrationUrl(task)} target="_top" rel="noreferrer">{label}</a>;
}
