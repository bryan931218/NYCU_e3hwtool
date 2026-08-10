import unittest
from types import SimpleNamespace

from e3_tracker.shared.study_progress_stability_runtime import (
    install_study_progress_stability_runtime,
)


class StudyProgressStabilityRuntimeTests(unittest.TestCase):
    def test_runtime_blocks_stale_tab_rewrites_and_unload_races(self):
        module = SimpleNamespace(
            STUDY_PLAN_TEMPLATE="""
<script>
const saveProgress = function (reason, useKeepalive, explicitOption) {
                const option = explicitOption || optionByValue(activeRecordId) || selectedOption();
                const youtubeId = option ? option.dataset.youtubeVideoId : '';
                if (!option || !youtubeId || !player || typeof player.getCurrentTime !== 'function') return Promise.resolve();
                if (queuedSaveCount && reason === 'interval') return Promise.resolve();
                    }).then(function (data) {
                        if (!applyProgressResult(data)) return;
                        playerLastSaved.textContent = new Date().toLocaleTimeString('zh-TW', { hour: '2-digit', minute: '2-digit' });
                        playerSyncState.textContent = data.stale
                            ? '偵測到較新的紀錄，已保留最新進度'
                            : reason === 'ended' ? '已完成並儲存' : '已自動儲存';
                    });
};
</script>
"""
        )

        install_study_progress_stability_runtime(module)

        self.assertIn("option.dataset.progressConflict === '1'", module.STUDY_PLAN_TEMPLATE)
        self.assertIn("useKeepalive && queuedSaveCount", module.STUDY_PLAN_TEMPLATE)
        self.assertIn("option.dataset.progressConflict = '1'", module.STUDY_PLAN_TEMPLATE)
        self.assertIn("stopSaveTimer();", module.STUDY_PLAN_TEMPLATE)
        self.assertIn("__e3StableVideoProgressSyncInstalled", module.STUDY_PLAN_TEMPLATE)

    def test_runtime_is_idempotent(self):
        module = SimpleNamespace(
            STUDY_PLAN_TEMPLATE="""
                if (!option || !youtubeId || !player || typeof player.getCurrentTime !== 'function') return Promise.resolve();
                if (queuedSaveCount && reason === 'interval') return Promise.resolve();
                    }).then(function (data) {
                        if (!applyProgressResult(data)) return;
                        playerLastSaved.textContent = new Date().toLocaleTimeString('zh-TW', { hour: '2-digit', minute: '2-digit' });
                        playerSyncState.textContent = data.stale
                            ? '偵測到較新的紀錄，已保留最新進度'
                            : reason === 'ended' ? '已完成並儲存' : '已自動儲存';
                    });
"""
        )
        install_study_progress_stability_runtime(module)
        once = module.STUDY_PLAN_TEMPLATE
        install_study_progress_stability_runtime(module)
        self.assertEqual(module.STUDY_PLAN_TEMPLATE, once)
        self.assertEqual(once.count("__e3StableVideoProgressSyncInstalled"), 1)


if __name__ == "__main__":
    unittest.main()
