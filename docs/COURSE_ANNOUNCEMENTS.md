# Course Announcements

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
services are required. Announcement updates do not send browser or LINE pushes.

Tests: `backend/tests/test_course_announcements.py` and
`frontend/tests/course-announcements.test.mjs`; run the normal backend and
frontend suites. Live E3 validation is required after deploying because course
themes and customized forum names can differ from the tested Moodle markup.
