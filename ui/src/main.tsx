import Dashboard from './screens/Dashboard';
import {installStyles} from './components/styles';

export type PluginProps = {dagId?: string; runId?: string; taskId?: string; mapIndex?: number};
function Cascade(_props: PluginProps = {}) {
  installStyles();
  return <div id="cascade-root"><Dashboard campaignId="api_v1_sunset" /></div>;
}

const globals = globalThis as typeof globalThis & {Cascade: typeof Cascade; AirflowPlugin: typeof Cascade};
globals.Cascade = Cascade;
globals.AirflowPlugin = Cascade;

export default Cascade;
