from e3_tracker.api import web
from e3_tracker.shared.deployment_runtime import install_deployment_runtime


install_deployment_runtime(web)
app = web.create_app()
