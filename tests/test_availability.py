from __future__ import annotations

import time
from datetime import UTC, datetime

import pytest

from agent_bridge.availability import UsageLimits, quota_blocked
from agent_bridge.registry import Registry


def future(seconds=3600):
    return datetime.fromtimestamp(time.time() + seconds, UTC).isoformat()


def test_limits_survive_restart_and_expire_without_claiming_quota(tmp_path):
    limits = UsageLimits(tmp_path)
    limits.set('kimi', None, 'monthly_limit', future())
    assert UsageLimits(tmp_path).active('kimi', None)
    assert not limits.active('kimi', None, now=time.time() + 3601)
    assert limits.clear('kimi', None)['cleared'] == 1


def test_account_and_model_scope_and_unknown_reset(tmp_path):
    limits = UsageLimits(tmp_path)
    limits.set('devin', 'swe-2-high', 'usage_limit', None)
    assert limits.active('devin', 'swe-2-high', now=time.time() + 9999999)
    assert not limits.active('devin', 'other')
    assert not limits.active('devin-well', 'swe-2-high')
    limits.set('devin', None, 'rate_limit', future())
    assert limits.active('devin', 'other')


def test_cooldown_cannot_shorten_operator_block(tmp_path):
    limits = UsageLimits(tmp_path)
    original = limits.set('kimi', None, 'monthly_limit', future(86400))
    assert limits.set('kimi', None, 'rate_limit', future(300), source='observed_error') == original


@pytest.mark.parametrize('value', ['2026-12-01', '2000-01-01T00:00:00Z', 'bad', '2100-01-01T00:00:00Z'])
def test_reset_requires_timezone_and_bounded_future(tmp_path, value):
    with pytest.raises(ValueError):
        UsageLimits(tmp_path).set('kimi', None, 'monthly_limit', value)


def test_corrupt_store_cannot_be_treated_as_available(tmp_path):
    limits = UsageLimits(tmp_path)
    limits.path.write_text('{bad')
    with pytest.raises(ValueError):
        limits.active('kimi', None)
    with pytest.raises(ValueError):
        limits.set('kimi', None, 'rate_limit', future())


def test_model_quota_does_not_exhaust_other_model():
    quota = dict(status='ok', stale=False, windows=[dict(name='weekly:opus', remaining_percent=0)])
    assert quota_blocked(quota, 'claude-opus-5-5')
    assert not quota_blocked(quota, 'claude-sonnet-5-5')
    assert not quota_blocked(dict(quota, stale=True), 'opus')


@pytest.mark.asyncio
async def test_blocked_dispatch_starts_no_worker_and_clear_refreshes(bridge_home, tmp_path, monkeypatch):
    registry = Registry.create(bridge_home)
    calls = []
    async def quota(*args, **kwargs):
        calls.append(True)
        return dict(status='unknown', stale=False)
    monkeypatch.setattr(registry, '_quota_for', quota)
    await registry.report_usage_limit('fake', 'monthly_limit', future())
    status = await registry.check_availability('fake')
    assert status['agents'][0]['status'] == 'blocked'
    assert not calls
    with pytest.raises(RuntimeError, match='retained usage limit'):
        await registry.dispatch_task('fake', 'must not start', cwd=str(tmp_path))
    assert not registry.tasks and not calls
    await registry.clear_usage_limit('fake')
    status = await registry.check_availability('fake')
    assert status['agents'][0]['status'] == 'unknown'
    assert calls


@pytest.mark.asyncio
async def test_nested_registry_cannot_change_limit(bridge_home):
    registry = Registry.create(bridge_home, runtime_context='worker')
    for action in (registry.report_usage_limit('fake', 'monthly_limit'), registry.clear_usage_limit('fake')):
        with pytest.raises(RuntimeError):
            await action


@pytest.mark.asyncio
async def test_runner_failure_retains_bounded_cooldown(bridge_home, tmp_path):
    from agent_bridge.models import Task, TaskStatus
    registry = Registry.create(bridge_home)
    task = Task(task_id="failure", session_id="session", agent="fake", message="work", cwd=str(tmp_path))
    task.status = TaskStatus.failed
    task.error = "rate limit exceeded"
    await registry.retain_usage_cooldown(task)
    limit = registry._usage_limits.active("fake", None)[0]
    assert limit["source"] == "observed_error"
    assert 0 < datetime.fromisoformat(limit["resets_at"]).timestamp() - time.time() <= 300
    task.agent = "other"
    task.error = "compilation failed"
    await registry.retain_usage_cooldown(task)
    assert not registry._usage_limits.active("other", None)


@pytest.mark.asyncio
async def test_model_limit_cannot_be_evaded_by_omitting_model(bridge_home, tmp_path, monkeypatch):
    registry = Registry.create(bridge_home)
    await registry.report_usage_limit('fake', 'usage_limit', model='specific-model')
    result = await registry.check_availability('fake')
    assert result['agents'][0]['model_required'] is True
    with pytest.raises(RuntimeError, match='retained usage limit'):
        await registry.dispatch_task('fake', 'no default escape', cwd=str(tmp_path))
    async def quota(*args, **kwargs):
        return dict(status='ok')
    monkeypatch.setattr(registry, '_quota_for', quota)
    result = await registry.check_availability('fake', 'other-model')
    assert result['agents'][0]['status'] == 'ready'


def test_concurrent_reports_do_not_lose_other_engine(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(lambda n: UsageLimits(tmp_path).set(str(n), None, 'usage_limit', future()), range(8)))
    assert len(rows) == len(UsageLimits(tmp_path).read()) == 8
