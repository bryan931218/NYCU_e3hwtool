# Study Authentication and Progress

- Study pages (`/admin/study-*`, `/study-progress`) use a separate, host-only, HTTP-only session cookie and signing salt. E3 assignment login/logout never replaces that cookie.
- A valid server-verified E3 administrator session bootstraps the study login. The study token is stored hashed in the existing `web_sessions` table, expires after 30 days, and contains no Moodle credential. Assignment sessions retain their original one-day lifetime.
- Study logout revokes only the study token and prevents automatic re-entry until the user explicitly resumes. All private study routes retain administrator checks and CSRF protection. Revoking a study token or its administrator flag still denies access.
- The video player checkpoints its position locally every five seconds and before pause, switching video, hiding or leaving the page. Normal server saves remain every 15 seconds. Study-time writes are also queued before transmission.
- Local backups are keyed by account, contain no authentication credentials, and are replayed after reload, reconnect or successful reauthentication. `X-Study-Account` prevents a stale player tab from writing under a different account. A refreshed CSRF token can be obtained without reloading the player.
- Optimistic progress versions remain authoritative. A stale backup never silently overwrites a newer server record; a conflicting local position is retained for explicit recovery. Late acknowledgments do not discard newer local snapshots. Study-time retries reuse the same cumulative session ID.
- If browser storage is disabled/full, the player reports that data is only held in the current tab. Clearing site storage or private-browser cleanup removes unsynced backups; another device has a separate local cache. Abrupt termination may lose the seconds since the most recent local checkpoint.

Verification: `backend/tests/test_study_session.py`, security/bootstrap tests, and `frontend/tests/study-progress-buffer.test.mjs`.
