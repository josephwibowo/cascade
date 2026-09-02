import type {AirflowTask} from '../api/client';

export function orchestrationUrl(task?: AirflowTask | null) {
  if (!task) return '/dags';
  // Airflow 3 routes a task instance by path, not by query string:
  // /dags/<dag>/runs/<run>/tasks/<task>/mapped/<index>. The older
  // `/grid?task_id=` shape resolves to the UI's 404 page.
  const run = `/dags/${encodeURIComponent(task.dag_id)}/runs/${encodeURIComponent(task.run_id)}/`;
  // Every task Cascade links to is dynamically mapped, and Airflow answers
  // `map_index: -1` on one of those with "No Task Instance found". Campaign
  // level links carry no index, so they open the run itself.
  if (task.map_index < 0) return run;
  return `${run}tasks/${encodeURIComponent(task.task_id)}/mapped/${task.map_index}`;
}

export default function ViewOrchestration({task, label = 'View Orchestration'}: {task?: AirflowTask | null; label?: string}) {
  return <a className="csc-action" data-primary="true" href={orchestrationUrl(task)} target="_top" rel="noreferrer">{label}</a>;
}
