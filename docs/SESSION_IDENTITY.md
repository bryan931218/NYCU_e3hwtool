# Session student-number enrichment

Session identities remain separate internal accounts. Student numbers are display
and statistics metadata only; enrichment never changes roles or merges credentials.

The reader follows the authenticated E3 user menu to its own profile. Edit links
must remain on the configured origin and `/user/edit.php`. A missing `id` is valid
for Moodle's own-profile form, but the form action and every hidden owner `id` must
match the profile's Moodle user ID. E3 can repeat this hidden field; missing,
conflicting, empty or editable owner fields are rejected. Duplicate links are skipped and at most three edit
pages are requested. No forms are submitted and no login redirects are followed.
An implicit own-profile menu link can provide a name; number lookup still requires
an explicit own Moodle ID from the authenticated menu, never an edit link alone.

Only the form's single `input#id_idnumber[name="idnumber"]` with `readonly` and nine
ASCII digits is accepted. Moodle's frozen fields do not always have `disabled`.
Editable fields, mismatched owners, unrelated numbers, names and course content
are never used to infer identity. Existing verified mappings cannot be replaced.

The authenticated profile name is saved independently of the student-number field.
Academic-unit labels are excluded, including E3's shortened 學士後 program labels
that omit a trailing 系 or 學程; ambiguous headings with multiple people remain blank.
If reading the edit form fails, the profile name is retained and the missing number
remains eligible for retry. A conflicting student number never changes either field.

The existing worker retries accounts missing a name as well as Session accounts
missing a student number, in batches of ten with a shared five-minute cooldown.
Name and student number are saved independently: failure to read the locked
number no longer discards an already verified own-profile name. Already identified
Session accounts and password accounts only need the name reader, not the edit form.
English profile names, grade labels and the alternate Moodle profile heading are
supported. Safe same-origin canonical redirects are bounded; login, external and
other-owner redirects remain rejected. Every attempt has a total timeout budget.
A fresh Session login forces an attempt. Legacy hour-long retry leases are
recognized and replaced on the next worker pass. Expired credentials
cannot be recovered automatically and require a new E3 login/session.

Enrichment writes only to an existing non-guest account and cannot recreate an
account deleted during a request, overwrite an established student identity, or
change administrator privileges. Retries never clear previously valid data.

Runtime diagnostics record only fixed failure codes, never usernames, student
numbers, names, URLs, HTML, cookies or upstream exception messages:

- `login_required`: E3 returned its login form or a login redirect.
- `access_denied`: E3 returned HTTP 403.
- `dashboard_unavailable`, `profile_unavailable`, `edit_unavailable`: unsuccessful response.
- `profile_link_missing`, `edit_link_missing`: missing or unsupported own links.
- `profile_owner_missing`: an implicit own profile has no authenticated menu owner ID.
- `edit_owner_mismatch`: no matching own edit form.
- `student_number_field_missing`, `student_number_unlocked`, `student_number_invalid`: field not usable.
- `upstream_timeout`, `upstream_request`, `internal_error`: transient/request/internal failure.
- `identity_conflict`: the saved identity could not accept the new value.

Backfill logs contain aggregate checked/updated counts only. An unavailable mapping
does not block assignment access. Tests cover both E3 and Moodle read-only formats,
implicit edit links, duplicate/bounded requests, wrong owners, redirects, malformed
fields, cooldown, retries and non-disclosure.
