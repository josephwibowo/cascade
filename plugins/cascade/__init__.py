from __future__ import annotations

import mimetypes
import sys
from pathlib import Path

from fastapi.staticfiles import StaticFiles
from airflow.plugins_manager import AirflowPlugin

# Airflow adds ``plugins`` itself to sys.path while loading plugins.  Because
# this plugin directory is named ``cascade``, it can otherwise shadow Astro's
# shared ``include/cascade`` package.  Extending the package path keeps both
# namespaces importable in the runtime image.
_shared = Path(__file__).parents[2] / "include" / "cascade"
__path__ = [str(Path(__file__).parent)]
if _shared.is_dir() and str(_shared) not in __path__:
    __path__.append(str(_shared))

from cascade.api.app import cascade_app

mimetypes.add_type("application/javascript", ".cjs")
cascade_app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static", html=True, check_dir=False), name="cascade_static")


class CascadePlugin(AirflowPlugin):
    name = "cascade"
    fastapi_apps = [{"app": cascade_app, "url_prefix": "/cascade", "name": "Cascade API"}]
    react_apps = [{"name": "Cascade", "bundle_url": "/cascade/static/cascade.umd.cjs", "destination": "nav", "url_route": "cascade"}]
