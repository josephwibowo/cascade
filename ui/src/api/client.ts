export type Campaign = {
  id: string; name: string; change_type: string; deadline: string; status: string;
  affected_accounts: number; affected_arr: number; migration_completion: number;
  status_distribution: Record<string, number>; risk_distribution: Record<string, number>;
  segment_distribution: Record<string, number>; blocked_accounts: number; pending_exceptions: number;
  airflow_dag_run_id?: string | null;
};
export type Account = {
  campaign_id: string; account_id: string; account_name: string; arr: number; tier: string; owner: string; region: string;
  status: string; segment: string; risk: string; blocker_type?: string | null; legacy_usage: number; replacement_usage: number;
  zero_v1_streak_days: number; daily_v1: number[]; daily_v2: number[]; latest_airflow_task_instance?: AirflowTask | null;
  brief?: Record<string, unknown> | null; brief_source?: string | null; evidence?: Record<string, unknown> | null;
};
export type AirflowTask = {dag_id: string; run_id: string; task_id: string; map_index: number};

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {headers: {'Content-Type': 'application/json'}, ...init});
  if (!response.ok) throw new Error((await response.text()) || `Request failed (${response.status})`);
  return response.json() as Promise<T>;
}
export const api = {
  campaign: (id: string) => request<Campaign>(`/cascade/campaigns/${encodeURIComponent(id)}`),
  accounts: (id: string, filters: Record<string, string>) => request<{items: Account[]}>(`/cascade/campaigns/${encodeURIComponent(id)}/accounts?${new URLSearchParams(filters)}`),
  account: (id: string, campaignId: string) => request<Account>(`/cascade/accounts/${encodeURIComponent(id)}?campaign_id=${encodeURIComponent(campaignId)}`),
  timeline: (id: string, accountId?: string) => request<{id: number; event_type: string; timestamp: string; summary: string; source: string; airflow_task_id?: string}[]>(`/cascade/campaigns/${encodeURIComponent(id)}/timeline${accountId ? `?account_id=${encodeURIComponent(accountId)}` : ''}`),
  exceptions: () => request<{items: {id: string; account_id: string; hitl_task_id?: string | null; airflow_dag_run_id?: string | null; status: string}[]; product_exception_count: number; airflow_awaiting_input: number | null; airflow_error?: string | null}>('/cascade/exceptions/pending'),
  respond: (id: string, chosen: string[], reason: string) => request(`/cascade/exceptions/${encodeURIComponent(id)}/respond`, {method: 'POST', body: JSON.stringify({chosen_options: chosen, reason})}),
  orchestration: (id: string) => request<Record<string, unknown>>(`/cascade/orchestration/${encodeURIComponent(id)}`),
  advance: (snapshot: string) => request<{snapshot: string; dag_run_id: string}>('/cascade/scenario/advance', {method: 'POST', body: JSON.stringify({snapshot})}),
};
