from e3_tracker.api import web
from e3_tracker.shared.deployment_runtime import install_deployment_runtime
from e3_tracker.shared.player_control_runtime import install_player_control_dock


install_deployment_runtime(web)
install_player_control_dock(web)
app = web.create_app()
