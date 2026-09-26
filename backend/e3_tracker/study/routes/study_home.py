"""Study home routes and their feature helpers."""

from flask import redirect, render_template_string, request, url_for


def register_study_home_routes(*,
    PUBLIC_STUDY_TEMPLATE,
    STUDY_HOME_TEMPLATE,
    STUDY_PLAN_SUBJECTS,
    _build_recall_widget_context,
    _load_study_progress_context,
    _repair_legacy_study_assistant_time_moves,
    admin_required,
    app,
    current_user,
):
    @app.get("/study-progress")
    def public_study_progress():
        user = current_user()
        context = _load_study_progress_context()
        return render_template_string(
            PUBLIC_STUDY_TEMPLATE,
            **context,
            share_url=request.url,
            is_admin=bool(user and user.get("is_admin")),
            admin_user=user,
            study_subjects=STUDY_PLAN_SUBJECTS,
        )

    @app.get("/public/study-progress")
    def public_study_progress_alias():
        return redirect(url_for("public_study_progress"), code=301)

    @app.get("/admin/study-home")
    @admin_required
    def admin_study_home():
        user = current_user()
        if user:
            _repair_legacy_study_assistant_time_moves(str(user.get("username") or ""))
        home_context = _load_study_progress_context()
        return render_template_string(
            STUDY_HOME_TEMPLATE,
            admin_user=user,
            recall_widget=_build_recall_widget_context(),
            **home_context,
        )

    return (
        public_study_progress,
        public_study_progress_alias,
        admin_study_home,
    )
