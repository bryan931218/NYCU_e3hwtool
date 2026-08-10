from __future__ import annotations

from typing import Any


_MARKER = "__e3StableVideoProgressSyncInstalled"

_SAVE_GUARD = """                if (!option || !youtubeId || !player || typeof player.getCurrentTime !== 'function') return Promise.resolve();
                if (queuedSaveCount && reason === 'interval') return Promise.resolve();
"""

_SAVE_GUARD_PATCHED = """                if (!option || !youtubeId || !player || typeof player.getCurrentTime !== 'function') return Promise.resolve();
                if (option.dataset.progressConflict === '1') {
                    playerSyncState.textContent = '其他頁面已有較新進度；此頁已停止自動寫入，重新載入後可接手';
                    return Promise.resolve();
                }
                // beforeunload cannot wait for the normal save queue. If another save is
                // already pending, sending a keepalive request with the same version can
                // race it and make the newest position stale. Prefer the queued save and
                // lose at most one autosave interval instead of moving progress backwards.
                if (useKeepalive && queuedSaveCount) return Promise.resolve();
                if (queuedSaveCount && reason === 'interval') return Promise.resolve();
"""

_STALE_HANDLER = """                    }).then(function (data) {
                        if (!applyProgressResult(data)) return;
                        playerLastSaved.textContent = new Date().toLocaleTimeString('zh-TW', { hour: '2-digit', minute: '2-digit' });
                        playerSyncState.textContent = data.stale
                            ? '偵測到較新的紀錄，已保留最新進度'
                            : reason === 'ended' ? '已完成並儲存' : '已自動儲存';
                    });
"""

_STALE_HANDLER_PATCHED = """                    }).then(function (data) {
                        if (data && data.stale) {
                            // A stale response means another tab/device has already moved
                            // this video's authoritative version. Do not let this old page
                            // adopt that version and then write its older player position
                            // back on the next 15-second autosave.
                            option.dataset.progressConflict = '1';
                            stopSaveTimer();
                        }
                        if (!applyProgressResult(data)) return;
                        playerLastSaved.textContent = new Date().toLocaleTimeString('zh-TW', { hour: '2-digit', minute: '2-digit' });
                        playerSyncState.textContent = data.stale
                            ? '偵測到其他頁面的較新紀錄；此頁已停止自動寫入，重新載入後可接手'
                            : reason === 'ended' ? '已完成並儲存' : '已自動儲存';
                    });
"""


def install_study_progress_stability_runtime(web_module: Any) -> None:
    """Prevent stale tabs and unload races from moving video progress backwards."""

    template = str(web_module.STUDY_PLAN_TEMPLATE)
    if _MARKER in template:
        return
    if _SAVE_GUARD not in template or _STALE_HANDLER not in template:
        return

    template = template.replace(_SAVE_GUARD, _SAVE_GUARD_PATCHED, 1)
    template = template.replace(_STALE_HANDLER, _STALE_HANDLER_PATCHED, 1)
    web_module.STUDY_PLAN_TEMPLATE = f"<!-- {_MARKER} -->\n{template}"
