# Site Boundaries

The E3 assignment tracker and graduate exam preparation site have separate
frontend and backend ownership. They still run in one deployment, with the
existing URLs, shared authentication and one database.

## Directory Map

| Area | E3 Assignment Tracker | Graduate Exam Preparation |
| --- | --- | --- |
| Templates and page fragments | `frontend/assignments/templates/` | `frontend/study/templates/` |
| Browser JS, CSS and images | `frontend/assignments/static/` | `frontend/study/static/` |
| Application wiring | `backend/e3_tracker/assignments/application.py` | `backend/e3_tracker/study/application.py` |
| HTTP endpoints | `backend/e3_tracker/assignments/routes/` | `backend/e3_tracker/study/routes/` |
| Integrations and services | `backend/e3_tracker/assignments/services/` | `backend/e3_tracker/study/services/` |
| Domain rules and data | `backend/e3_tracker/assignments/domain/` | `backend/e3_tracker/study/domain/` |
| Repositories and tables | `backend/e3_tracker/assignments/persistence/` | `backend/e3_tracker/study/persistence/` |

E3 owns assignment collection, course/semester filtering, preferences, exports,
Google Calendar and the E3 profile integration. Study owns schedules, videos,
playback, notes, search, recall, AI assistants and Discord study presence.

## Shared Platform

- `frontend/shared/` contains theme tokens, base components, theme initialization,
  legal pages and shared administration pages. It contains no site-specific UI.
- `backend/e3_tracker/platform/` owns account sessions, access checks, environment
  configuration, traffic, announcements, feedback, assets and database infrastructure.
- `backend/e3_tracker/bootstrap.py` assembles the deployment. Platform application
  wiring installs the two site registrars explicitly; study feature registration
  is idempotent and application-local.
- `platform/storage.py` and `application_storage.py` compose the repositories for
  the existing shared database. Site repositories import their own schema and
  the shared account schema, not the other site's repositories or tables.
- `platform/persistence/schema.py` is the deployment-wide table registry, not a
  place to add product tables. Table names and migration IDs remain unchanged.
- Each site's `persistence/migrations.py` owns its legacy column upgrades; the
  platform migration runner composes them under the existing ordered history.
- Player preferences, Discord presence and study data repairs live in the study
  domain/services/repositories. `platform/deployment_runtime.py` only composes
  their repository mixin with the shared storage infrastructure.

## Editing Rules

1. Add each site's UI only to its own frontend directory. Shared visual primitives
   belong in `frontend/shared/`; browser behavior does not belong in backend routes.
2. Keep request parsing/access checks in routes, orchestration and integrations in
   services, domain rules in `domain`, and SQL/data access in `persistence`.
3. Neither site imports the other. Cross-site navigation uses URLs; deployment
   composition injects any needed platform services.
4. Jinja references are namespaced, for example
   `assignments/components/assignments.html` and `study/components/study_assets.html`.
5. Assets use `/assets/assignments/...`, `/assets/study/...` or `/assets/shared/...`.
   The old Discord cover URL remains a compatibility alias.
6. Update database schemas under their owning site. Keep upgrades additive and
   retain existing migration history; a directory move is not a data migration.

## Development and Checks

Continue using `python start_servers.py`: frontend proxy on port 3000, backend on
port 8000. No new service, environment variable or separate deployment is required.

```powershell
python -m unittest discover -s backend/tests -t backend
```

`test_site_boundaries.py` checks imports, table ownership, template namespaces,
route ownership and frontend asset isolation. Existing behavior tests cover both
sites. The playlist sync command is unchanged:

```powershell
python backend/tools/sync_youtube_playlists.py --sync-db
```

Its inventory now belongs to `backend/e3_tracker/study/domain/study_plan_videos.json`.
