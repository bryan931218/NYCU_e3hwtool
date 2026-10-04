# Course Announcements

## Separate Course Mail

`/courses/mail` reads E3's dcpcmail course inbox; `/courses/announcements` continues
to read Moodle news forums. Navigation, API endpoints, cache tables, and local
read markers remain separate even when message IDs overlap. The two readers
share the same presentation and combined two-operation concurrency limit.

Mail synchronization uses `/local/dcpcmail/view.php?c=COURSE&t=inbox` and reads the
first inbox page, up to 30 messages per course. It only uses the signed-in
account's current course catalog and session. Unrecognized inbox/body markup
is an error, not an empty inbox; failures preserve previous data. No send,
reply, or delete operations are implemented. Older mail remains available in E3.

Migration `0013_course_mail` adds a separate cache. Subjects, senders, message
content, and read markers are encrypted with the existing data-encryption key,
bound to the account and semester. Account deletion removes the mail cache.
No additional credentials or environment variables are required.

Unlike announcements, desktop mail does not automatically open the first item.
The user must click a message to retrieve its content. Opening a mail detail
may mark it read in E3 itself. Tracker read/unread controls change only the
tracker's state; initial state is taken from the E3 inbox.

Protocol/DOM reference: the original implementation of E3 dcpcmail integration
in [NYCU portal_e3_helper](https://github.com/NYCU-Chung/portal_e3_helper/blob/main/content.js).
The live authenticated inbox, empty current-course inbox and an already-read
message's body selectors were verified on 2026-10-04. Additional tests are in
`backend/tests/test_course_mail.py`.

The assignment site's `/courses/announcements` page uses the current account's
existing Moodle session. It does not access another account through `view_user`
or accept arbitrary remote URLs from the browser. Guest accounts cannot sync.

The course catalog comes from the existing assignment cache. News forums are
discovered through the course page's announcement/news labels or latest-news
block. Student Q&A forums are excluded. The first page of each news forum is
cached, up to 30 discussions per course; older posts remain available in E3.

On opening the page, a cache older than ten minutes is refreshed in the
background. Manual updates have a one-minute cooldown. Two operations per
application process may access E3 concurrently. Synchronization has a one-minute
budget; failed courses retain their previous cache. Content is fetched only when
opening an announcement, then cached as plain text with safe HTTPS links.
The parser supports legacy Moodle posts and Moodle 3.11/4.x post-content
containers, first-post scoping, modern file/image attachments, and author/date
markers. Image-only announcements expose image links rather than loading remote
images automatically.

The desktop UI uses an inbox and reading pane; mobile opens the reading pane
with a return-to-list action. Successfully opening content marks it as read.
Manual read/unread changes do not depend on a successful remote content fetch.
Failed reads keep the unread state and expose an explicit retry action.

Migration `0012_course_announcements` creates the account/semester cache table.
Read status is local to this tracker, not Moodle's read-tracking state. Deleting
an account also removes its announcement cache. No new environment variables or
services are required. The workbench has one course-message entry at
`/courses/messages`; announcements and mail retain separate tabs, APIs, caches,
and read markers. The former URLs remain compatible.

The entry and source tabs show a red dot for unread cached messages in the
selected semester. `/api/course-messages/unread` exposes counts only, scoped to
the signed-in account and its current course catalog. It never calls E3. The
workbench polls this small summary once a minute while visible, and
on semester changes or return from the reader; reading/unreading updates tab dots.

## Opt-in Message Notifications

Notification settings have independent `new_announcement` and `new_mail` switches,
both off by default. Existing browser subscriptions and LINE bindings are reused.
The bounded background worker checks only the account's current-semester courses.
First successful synchronization per course/source establishes a baseline, even
for an empty inbox. Failed courses never establish a baseline or erase old data.
Only previously unseen IDs with a known timestamp within the last day trigger an
alert; edits, historical semesters and old messages revealed later do not.

Cache updates, seen IDs and outbox insertion commit atomically. Alert payloads
are encrypted at rest and contain only the course name, subject and an own-account
deep link, never the private body or sender. Delivery rechecks preferences and
channel ownership. Disabling a channel, unlinking LINE or deleting a device/account
prevents further delivery. Settings saves baseline existing cached messages.

The live E3 empty inbox uses `.mail_list > .mail_item` containing
`沒有可查看的郵件.` rather than a message link. This is a valid empty result,
not a malformed mail row. Unknown pages and malformed mixed rows still fail closed.

Tests: `backend/tests/test_course_announcements.py` and
`frontend/tests/course-announcements.test.mjs`; run the normal backend and
frontend suites. Live E3 validation is required after deploying because course
themes and customized forum names can differ from the tested Moodle markup.
