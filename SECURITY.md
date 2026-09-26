# Security and Public Deployment

This hardening addresses confirmed application issues. It is not a guarantee of
zero vulnerabilities or a substitute for a deployed-system security assessment.
The existing shared deployment and URLs are retained; the two products remain
separate application modules.

## Before Deploying

1. Back up the database and private uploads. Restrict backup access and encryption
   keys separately; course data and notes are still sensitive even when API tokens
   are encrypted. Confirm recovery before changing production.
2. Configure the hosting provider's secret manager using
   `.env.production.example` as a checklist. Do not commit real `.env` files.
3. Generate a fresh random `E3_WEB_SECRET` with
   `python -c "import secrets; print(secrets.token_urlsafe(48))"`.
4. Generate the independent `E3_DATA_ENCRYPTION_KEY` with
   `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.
   Keep this key stable across restarts and replicas. Losing it makes stored
   credentials unreadable. Do not derive it from or reuse the signing key.
5. Set `E3_ENV=production`, `E3_SESSION_COOKIE_SECURE=1`,
   `E3_SESSION_COOKIE_SAMESITE=Lax`, and `E3_DEV_RELOAD=0`.
   Set `E3_TRUSTED_HOSTS` to the actual public domains and any health-check host
   used by your provider. Railway requires `healthcheck.railway.app`; `/healthz`
   intentionally does not redirect to the canonical website. No wildcard is
   needed for the normal deployment.
   Set `E3_ADMIN_USER_ID` explicitly to the intended administrator's E3 account.
6. Set `E3_PROXY_HOPS` to the exact trusted reverse-proxy count. A value of `1`
   is appropriate only when exactly one trusted proxy supplies the forwarding
   headers. Block direct access to the origin; an exposed origin with trusted
   forwarded headers allows spoofed client addresses and rate-limit bypass.
7. Use the updated Procfile/Gunicorn WSGI entry point. Do not run Flask's debug
   server publicly. Development servers now bind only to loopback. Use a supported,
   patched Python runtime and bootstrap `requirements-build.txt` before installing
   `requirements.txt`; fresh virtual environments may bundle old, vulnerable
   pip/setuptools. The application requirements also retain the audited tool
   versions for providers with automatic requirements installation.
8. Require HTTPS at the edge, preserve the application's security headers,
   disable caching of authenticated/private endpoints, and verify HTTPS OAuth
   callback URLs. Keep database ports private, use TLS where required, and use
   least-privilege database and hosting accounts with MFA.

Run `python backend/tools/check_security.py` inside the deployment environment
before starting it. This checks configuration without printing secret values or
modifying the database. Add `--url https://www.e3hwtool.space/login` after deployment
for a read-only HTTP header check. This is not a penetration test.

## Migration Behavior

- Migration `0004_security` adds trusted server-side session fields and persistent
  rate-limit counters. Existing login sessions are invalidated; users must log in
  again. Assignment data, study progress, notes, and unrelated records are kept.
- Browser cookies no longer contain MoodleSession, usernames, or role claims.
  The opaque login token is hashed in the database; server sessions expire after
  24 hours and can be revoked. Production uses a Secure, HttpOnly, SameSite=Lax,
  host-only `__Host-e3_session` cookie.
- MoodleSession and Google access/refresh tokens use authenticated Fernet
  encryption with account-and-field binding. Existing plaintext Google tokens
  are encrypted transactionally at startup. A wrong encryption key refuses
  startup instead of overwriting credentials.
- Encryption does not repair old backups, leaked tokens, or earlier cookie
  disclosure. If the old default signing key was used publicly, rotate it,
  invalidate old E3 sessions, review admin access, and revoke/re-authorize Google
  grants where exposure is suspected. Rotate other exposed API/OAuth secrets.
- Old note search/image URLs now require a trusted administrator. Public study
  progress remains public, but anonymous visitors cannot search private notes or
  fetch raw images. Purge any previous CDN image cache. Already downloaded data
  cannot be recalled.
- The frontend clears previously remembered passwords/MoodleSession from
  localStorage on the next page load. Only the username can be remembered.
  Existing browser profiles that never revisit the site still need their old
  stored credentials cleared manually.

## Request Protections

All POST/PUT/PATCH/DELETE requests require a session-bound CSRF token; forms and
same-origin fetch calls supply it. The token is bound to the browser session
without an additional time limit so long study pages can submit safely; a new
login clears the old session. Cross-origin mutation requests are also rejected.
Logout is POST-only. Google OAuth state must match the initiating browser and is
consumed once. The Discord agent retains its GET bearer-token API; it does not
receive a cookie-authenticated CSRF exemption.

The current default limits are 3000 requests/minute/IP and 120 login attempts per
5 minutes/IP, plus 20 attempts per 5 minutes/account and 10 per browser session.
The broader IP limit accommodates campus NAT without relaxing the separate account
limit. Configure them with `E3_REQUESTS_PER_MINUTE`, `E3_LOGIN_IP_LIMIT`,
`E3_LOGIN_ACCOUNT_LIMIT`, and `E3_LOGIN_BROWSER_LIMIT`; counters are shared through
the database. Review these limits against actual campus/shared-network traffic
before a large rollout; app-level limits are not DDoS protection. Apply edge rate limits,
upload quotas, and spending limits for external AI/media services as well.

Private responses are not publicly cacheable. Headers include nonce-based CSP,
frame blocking, MIME sniffing protection, referrer/permissions policies, and HSTS
in production. Inline script handlers are removed. Existing inline CSS remains
allowed by `style-src`; scripts do not have an `unsafe-inline` allowance. CDN
libraries use pinned versions and integrity checks where possible. YouTube's
dynamically loaded API is an external trust dependency.

Uploads are size-limited and validated as real images with matching formats and
bounded dimensions. New note uploads accept at most 50 images. Private original
images may contain metadata; do not intentionally publish them without reviewing
and removing sensitive metadata. Imported assignment links reject executable URL
schemes, and login toast messages are inserted as text rather than HTML.

## Maintenance and Verification

`requirements-security-constraints.txt` records audited deployment versions. When
upgrading dependencies, update the constraints deliberately and run:

```sh
python -m pip install -r requirements-build.txt
python -m pip install -r requirements.txt 'pip-audit>=2.10.1,<3'
python -m unittest discover -s backend/tests -t backend
python -m pip_audit
```

The security GitHub workflow runs tests and an audit on pushes/PRs. New security
tests use normal clients with protections enabled. Existing functional tests send
valid CSRF headers; they do not disable runtime checks.

Before opening registration broadly, also verify production routing, reverse-proxy
trust, database/volume permissions, encrypted backups, secret rotation, incident
logs, and third-party account permissions. Review public study metadata and the
AI processing disclosure. Neither passing tests nor a clean dependency audit
proves that a system has no security issues.

References: [Flask security](https://flask.palletsprojects.com/en/stable/web-security/),
[proxy trust](https://flask.palletsprojects.com/en/stable/deploying/proxy_fix/),
[Flask-WTF CSRF](https://flask-wtf.readthedocs.io/en/1.2.x/csrf/),
[Fernet and rotation](https://cryptography.io/en/latest/fernet/),
[pip security fixes](https://pip.pypa.io/en/stable/news/),
[setuptools advisory](https://github.com/pypa/setuptools/security/advisories/GHSA-h35f-9h28-mq5c).
