from e3_tracker.api import web
from e3_tracker.shared.deployment_runtime import install_deployment_runtime
from e3_tracker.shared.player_control_runtime import install_player_control_dock
from e3_tracker.shared.study_calendar_runtime import install_study_calendar_runtime


install_deployment_runtime(web)
install_player_control_dock(web)
install_study_calendar_runtime(web)
app = web.create_app()
