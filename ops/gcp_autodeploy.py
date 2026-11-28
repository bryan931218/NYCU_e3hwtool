"""Deploy validated GitHub main commits from the production VM."""

import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen


REPO = 'bryan931218/NYCU_e3hwtool'
ORIGIN = f'https://github.com/{REPO}.git'
BRANCH = 'refs/heads/main'
CHECKS = {352378898: 'Backend validation', 367873352: 'Security validation'}
ROOT = Path.home() / '.e3hwtool-release'
WEB = 'e3hwtool-release-web-1'
WORKER_PREFIX = 'e3hwtool-notifications-'
HOSTNAME = 'www.e3hwtool.space'
SHA = re.compile(r'[0-9a-f]{40}')


def run(args, *, timeout=45, **options):
    return subprocess.run(args, check=True, stdout=subprocess.PIPE, timeout=timeout, **options).stdout


def main_revision():
    row = run(['git', 'ls-remote', '--exit-code', ORIGIN, BRANCH], timeout=20).decode().split()
    if len(row) != 2 or row[1] != BRANCH or not SHA.fullmatch(row[0]):
        raise RuntimeError('GitHub did not return a valid main revision')
    return row[0]


def validation_results(revision):
    query = urlencode({'branch': 'main', 'event': 'push', 'head_sha': revision, 'per_page': 50})
    request = Request(f'https://api.github.com/repos/{REPO}/actions/runs?{query}', headers={
        'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'E3hwtool production deployment',
    })
    with urlopen(request, timeout=20) as response:
        runs = json.load(response)['workflow_runs']
    latest = {}
    for item in runs:
        workflow = item.get('workflow_id')
        if (workflow in CHECKS and item.get('head_sha') == revision and item.get('event') == 'push'
                and item.get('head_branch') == 'main'
                and (item.get('head_repository') or {}).get('full_name', '').casefold() == REPO.casefold()):
            previous = latest.get(workflow)
            if previous is None or (item['id'], item.get('run_attempt', 1)) > (
                previous['id'], previous.get('run_attempt', 1),
            ):
                latest[workflow] = item
    return {name: latest.get(workflow, {}).get('conclusion') or 'waiting' for workflow, name in CHECKS.items()}


def inspect_container(name):
    return json.loads(run(['docker', 'inspect', name]))[0]


def container_revision(container):
    key = 'RAILWAY_GIT_COMMIT_SHA='
    value = next((entry[len(key):] for entry in container['Config']['Env'] if entry.startswith(key)), '')
    if not SHA.fullmatch(value):
        raise RuntimeError('Production container has no valid release revision')
    return value


def compose_command(files):
    return ['docker', 'compose', *(arg for file in files for arg in ('-f', str(file)))]


def wait_for_health(container, revision):
    path = f'https://{HOSTNAME}/healthz'
    # Probe the application directly first, and then its normal TLS route.
    command = ('import json, urllib.request; '
               f'request=urllib.request.Request("http://127.0.0.1:8000/healthz", headers={{"Host":"{HOSTNAME}"}}); '
               'print(json.load(urllib.request.urlopen(request, timeout=4))["release"])')
    probe = ['docker', 'exec', container, 'python', '-c', command]
    if container is None:
        probe = ['curl', '-fsS', '--max-time', '5', '--resolve', f'{HOSTNAME}:443:127.0.0.1', path]
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        result = subprocess.run(probe, capture_output=True, timeout=8)
        if result.returncode == 0:
            response = result.stdout.decode().strip()
            reported = json.loads(response).get('release') if container is None else response
            if reported == revision[:12]:
                return
        time.sleep(2)
    raise RuntimeError(f'Health check failed for {container or HOSTNAME}')


def build(revision, active):
    repo = ROOT / 'deployment-source.git'
    if not repo.exists():
        run(['git', 'init', '--bare', str(repo)])
    git = ['git', '--git-dir', str(repo)]
    run(git + ['fetch', '--no-tags', ORIGIN, f'+{BRANCH}:refs/deploy/main'], timeout=150)
    if run(git + ['rev-parse', 'refs/deploy/main']).decode().strip() != revision:
        raise RuntimeError('Main advanced before build; the timer will check the new revision')
    run(git + ['merge-base', '--is-ancestor', active, revision])
    image = f'e3hwtool:{revision[:12]}'
    with tempfile.TemporaryDirectory(prefix='source-', dir=ROOT) as directory, tempfile.TemporaryFile() as archive:
        subprocess.run(git + ['archive', revision], stdout=archive, check=True, timeout=60)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as source:
            source.extractall(directory, filter='data')
        with (ROOT / 'autodeploy-build.log').open('wb') as log:
            subprocess.run(['docker', 'build', '--progress=plain', '-t', image, directory],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=900)
    return image


def activate(revision, image, current):
    previous = container_revision(current)
    labels = current['Config']['Labels']
    previous_files = [Path(file) for file in labels['com.docker.compose.project.config_files'].split(',')]
    previous_compose = compose_command(previous_files)
    base_files = [file for file in previous_files if not file.name.startswith('compose.release-')]
    override = ROOT / f'compose.release-{revision[:12]}.json'
    override.write_text(json.dumps({'services': {'web': {'image': image, 'environment': {'RAILWAY_GIT_COMMIT_SHA': revision}}}}))
    compose = compose_command(base_files + [override])
    run(compose + ['config', '--quiet'])
    run(compose + ['run', '--rm', '--no-deps', '-T', '--entrypoint', 'python', 'web',
                   '/app/backend/tools/check_security.py'])
    names = run(['docker', 'ps', '--format={{.Names}}']).decode().splitlines()
    workers = [name for name in names if name.startswith(WORKER_PREFIX)]
    if len(workers) != 1:
        raise RuntimeError('Expected exactly one active notifications worker')
    old_worker = workers[0]
    new_worker = WORKER_PREFIX + revision[:12]
    # A failed attempt may have left this revision's one-off worker stopped.
    if new_worker in run(['docker', 'ps', '-a', '--format={{.Names}}']).decode().splitlines():
        run(['docker', 'rm', new_worker])
    web_changed = False
    worker_stopped = False
    try:
        run(['docker', 'stop', '-t', '12', old_worker])
        worker_stopped = True
        run(compose + ['run', '--no-deps', '-T', '-d', '--name', new_worker,
                       '-e', 'E3_NOTIFICATIONS_WORKER=1', 'web'])
        wait_for_health(new_worker, revision)
        run(['docker', 'update', '--restart', 'unless-stopped', new_worker])
        web_changed = True
        run(compose + ['up', '-d', '--no-deps', 'web'])
        wait_for_health(WEB, revision)
        wait_for_health(None, revision)
    except Exception:
        # Always restore the worker, even if restoring the HTTP path needs intervention.
        try:
            if web_changed:
                run(previous_compose + ['up', '-d', '--no-deps', 'web'])
                wait_for_health(None, previous)
        finally:
            if worker_stopped:
                subprocess.run(['docker', 'stop', '-t', '8', new_worker], capture_output=True, timeout=20)
                run(['docker', 'start', old_worker])
        raise
    print(f'Deployed {revision[:12]} to HTTPS and the notification worker', flush=True)


def deploy_if_ready():
    current = inspect_container(WEB)
    active = container_revision(current)
    revision = main_revision()
    if revision == active:
        print(f'Production is current at {active[:12]}', flush=True)
        return
    failed = ROOT / 'autodeploy-failure.json'
    previous_failure = json.loads(failed.read_text()) if failed.exists() else {}
    if previous_failure.get('revision') == revision and time.time() - previous_failure['time'] < 1800:
        print(f'{revision[:12]} is in its deployment retry cooldown', flush=True)
        return
    results = validation_results(revision)
    print(f'{revision[:12]} checks: ' + ', '.join(f'{name} {result}' for name, result in results.items()), flush=True)
    if any(result != 'success' for result in results.values()):
        return
    try:
        image = build(revision, active)
        if main_revision() != revision:
            print('Main advanced during build. Waiting for the latest commit.', flush=True)
            return
        activate(revision, image, current)
    except Exception:
        failed.write_text(json.dumps({'revision': revision, 'time': time.time()}))
        raise
    failed.unlink(missing_ok=True)


if __name__ == '__main__':
    os.umask(0o077)
    os.chdir(ROOT)
    with (ROOT / 'application-release.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        deploy_if_ready()
