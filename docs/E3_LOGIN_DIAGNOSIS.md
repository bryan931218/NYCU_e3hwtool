# E3 login investigation (2026-11-24)

## Observed failure

The successful browser login supplied by the operator starts at
`https://portal.nycu.edu.tw/#/login?redirect=%2Fredirect%2Fnewe3p`.
The tracker's password client instead submits to E3's local
`https://e3p.nycu.edu.tw/login/index.php` form. It did not implement the Portal
authentication flow. The pre-investigation HTTP login implementation had not
changed since September 27.

A loopback-only diagnostic of the affected account confirmed that the outgoing
username, password and login token matched the form input; the Moodle cookie was
sent once with the correct host and path. E3 answered the credential POST with
303 to `/`, then redirected back to its login page. It did not rotate the login
cookie, perform its `testsession` check, or expose an authenticated-user marker.
The same failure occurred locally and on Railway. No raw credentials, cookies,
response bodies or personal data were retained in the diagnostic logs.

A positive control using E3's publicly provided guest form completed the same
HTTP transport, rotated the cookie and passed E3's session-test redirect. This
rules out a universal failure of the Python transport or its cookie handling;
it does not prove why the affected account's local authenticator rejects it.

The current authentication-path mismatch is demonstrated. The exact historical
change that made a previously compatible account fail is not established by
client-side evidence; school authentication audit/configuration history would
be needed to attribute that change. Do not report a school maintenance or password
change as the proven cause.

## School login return flow

Extension version 1.2.0 adds a user-initiated administrator school-login flow.
The tracker opens the actual Portal route, lets the user complete school login,
and resumes the original tracker document after E3 confirms authentication.
This uses the existing E3 cookie permission; it does not collect Portal passwords,
Portal tokens, MFA codes, or add Portal host access.

Each transfer is scoped to the initiating login-page document, a one-use request
ID, the same browser cookie store and a ten-minute deadline. Extension session
storage contains only navigation metadata. The E3 credential is transferred in
memory to the initiating content script, then sent to the tracker's existing
CSRF-protected, same-origin administrator Session login. The server independently
revalidates the configured immutable Moodle principal on every login and reuses
the original administrator account. Client-reported authentication alone cannot
grant a role. Failed/MFA/policy states do not transfer a credential.

The native-password option remains available for accounts accepted by E3's local
authenticator. Browser school-login return requires the updated helper. An old
helper leaves the new button disabled instead of submitting school credentials
to the wrong authenticator.

End-to-end browser verification is required in addition to unit tests and a
successful deployment before reporting that this flow has restored access.
