# Session student-number enrichment

Session identities remain separate internal accounts. Student numbers are display
and statistics metadata only; enrichment never changes roles or merges credentials.

The reader follows the authenticated E3 user menu to its own profile. Edit links
must remain on the configured origin and `/user/edit.php`. A missing `id` is valid
for Moodle's own-profile form, but the form action and hidden owner `id` must match
the profile's Moodle user ID. Duplicate links are skipped and at most three edit
pages are requested. No forms are submitted and no login redirects are followed.

Only the form's single `input#id_idnumber[name="idnumber"]` with `readonly` and nine
ASCII digits is accepted. Moodle's frozen fields do not always have `disabled`.
Editable fields, mismatched owners, unrelated numbers, names and course content
are never used to infer identity. Existing verified mappings cannot be replaced.

Active unresolved sessions are retried by the existing worker, in batches of ten
with a shared one-hour cooldown. A fresh login forces an attempt. Expired credentials
cannot be recovered automatically and require a new E3 login/session.

Runtime diagnostics record only fixed failure codes, never usernames, student
numbers, names, URLs, HTML, cookies or upstream exception messages:

- `login_required`: E3 returned its login form or a login redirect.
- `access_denied`: E3 returned HTTP 403.
- `dashboard_unavailable`, `profile_unavailable`, `edit_unavailable`: unsuccessful response.
- `profile_link_missing`, `edit_link_missing`: missing or unsupported own links.
- `edit_owner_mismatch`: no matching own edit form.
- `student_number_field_missing`, `student_number_unlocked`, `student_number_invalid`: field not usable.
- `upstream_timeout`, `upstream_request`, `internal_error`: transient/request/internal failure.
- `identity_conflict`: the saved identity could not accept the new value.

Backfill logs contain aggregate checked/updated counts only. An unavailable mapping
does not block assignment access. Tests cover both E3 and Moodle read-only formats,
implicit edit links, duplicate/bounded requests, wrong owners, redirects, malformed
fields, cooldown, retries and non-disclosure.
