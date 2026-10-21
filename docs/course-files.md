# Course Attachment Library

The assignment site owns `/courses/files` and `/api/course-files`. Navigation is
available in the existing course-message tabs and the workbench operations menu.
No attachment module or remote scan is loaded by the assignment workbench.

## Finding Files

The primary organization is semester, then course. Desktop shows a course
sidebar; mobile uses the existing accessible course picker. Results are grouped
under readable course names, with source tabs for materials, assignments,
announcements and mail. File type and sorting stay inside the optional filter
panel. Duplicate course/URL entries retain their origins in one expandable row.
Search matches filenames, course and origin titles, ignoring case, full-width
characters and filename separators. LINE links can preselect a course and query.

## LINE Lookup

- Send `找附件` to browse courses, or `找附件 HW2` to search the bound account.
  Results use Flex messages with five files per page and eight courses per page.
- Website and LINE use the same semester-scoped catalog and encrypted index.
  LINE prefers the current semester; normal searches do not fetch E3, mark
  messages read, or change automatic notification preferences.
- The explicit update action starts the existing bounded background scan, using
  only the latest unexpired web session belonging to the currently bound account.
  Expired E3 credentials keep cached files searchable but require website login
  before refreshing. Existing reply buttons let the user check results later;
  no unsolicited completion push is sent.
- Browsing actions expire after 15 minutes and are HMAC-bound to the LINE user
  and website account. Every action rechecks binding, course and semester scope;
  group events and redeliveries are ignored. Lookup and refresh have separate
  per-account rate limits. No credentials are embedded in buttons or file links.
- E3 file links open the external browser; third-party signed URLs remain intact.
  Links exceeding LINE's URI limit fall back to the scoped web library. No file
  content is uploaded to LINE or downloaded by this lookup service.

## Data and Access

- Each request uses the authenticated account and its cached semester catalog.
  Guests and admin read-only inspection cannot select somebody else's library.
- The additive `0018_course_files` migration creates a separate encrypted index
  per account and semester. It stores file/link metadata, not file binaries.
- The index combines course resources, folders, assignment introductions,
  announcement attachments, and links already present in the private mail cache.
  Opening the library never marks local messages read or acknowledges badges.
- Unread mail bodies are not fetched by indexing. Mail attachments become
  available when the existing message reader retrieves their content.
- Moodle download URLs open directly in E3 and require the browser's own E3
  login. The site does not forward its MoodleSession or proxy downloads.
- Remote HTML is parsed into text and HTTPS links only. School fetch URLs are
  reconstructed from validated course/module IDs; external links are never
  fetched by the server. Session-token links and submission/feedback files are
  excluded. Account deletion removes the encrypted index as well.

## Bounded Updating

Only first entry into an uninitialized semester, or the update button, starts a
scan. The scanner shares the two existing per-process Moodle synchronization
slots and stops after 55 seconds or 60 pages. Pending sources are persisted for
the next update. Successful sources replace their old links; failed sources
retain prior metadata. Claims are transactional and late workers cannot replace
a newer refresh or recreate a deleted account.

School responses retain the existing 2 MiB limit. Discovery accepts up to 150
activities and 100 links per parent, messages retain the existing 30-summary/20-link parser
limits, and an API response shows up to 3000 distinct course/URL entries. Activity
or response truncation is shown explicitly. The browser renders 50 entries at a
time. Polling runs only while a scan is active and the tab is visible.

## Checks

```powershell
python -m unittest discover -s backend/tests -t backend
python -m unittest discover -s backend/tests -t backend -p test_line_files.py
npm --prefix frontend test
node backend/tests/course_files_browser.cjs
node backend/tests/workbench_loading_browser.cjs
```

Browser scripts use synthetic fixtures, need Playwright on Node's module path,
and write desktop/mobile screenshots under the ignored `output/playwright/`.
They never use a real account or make requests to E3.
