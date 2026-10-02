(() => {
    const dialog = document.getElementById('sessionHelpModal');
    const modeButton = document.querySelector('[data-mode="session"]');
    const helpButton = document.getElementById('sessionHelpButton');
    const closeButton = document.getElementById('sessionGuideClose');
    const previousButton = document.getElementById('sessionGuidePrev');
    const nextButton = document.getElementById('sessionGuideNext');
    const nextLabel = document.getElementById('sessionGuideNextLabel');
    const counter = document.getElementById('sessionGuideCount');
    const progress = document.getElementById('sessionGuideProgress');
    const sessionInput = document.getElementById('moodle_session');
    if (!dialog || !modeButton || !helpButton || !nextButton) return;

    const steps = Array.from(dialog.querySelectorAll('[data-guide-step]'));
    const scenes = Array.from(dialog.querySelectorAll('[data-guide-scene]'));
    const seenKey = 'e3_session_guide_seen_v1';
    let shownThisPage = false;
    let currentStep = 0;

    function hasSeenGuide() {
        if (shownThisPage) return true;
        try { return localStorage.getItem(seenKey) === '1'; }
        catch (err) { return false; }
    }

    function showStep(index) {
        currentStep = Math.max(0, Math.min(steps.length - 1, index));
        steps.forEach((step, i) => { step.hidden = i !== currentStep; });
        scenes.forEach((scene, i) => { scene.hidden = i !== currentStep; });
        counter.textContent = `${String(currentStep + 1).padStart(2, '0')} / ${String(steps.length).padStart(2, '0')}`;
        progress.style.width = `${(currentStep + 1) * 100 / steps.length}%`;
        previousButton.hidden = currentStep === 0;
        nextLabel.textContent = currentStep === steps.length - 1 ? '開始貼上' : '下一步';
    }

    function openGuide() {
        if (dialog.open) return;
        shownThisPage = true;
        try { localStorage.setItem(seenKey, '1'); } catch (err) {}
        showStep(0);
        dialog.showModal();
        closeButton.focus();
    }

    modeButton.addEventListener('click', () => {
        if (!hasSeenGuide()) openGuide();
    });
    helpButton.addEventListener('click', () => {
        if (!modeButton.classList.contains('active')) modeButton.click();
        openGuide();
    });
    closeButton.addEventListener('click', () => dialog.close());
    previousButton.addEventListener('click', () => {
        showStep(currentStep - 1);
        if (previousButton.hidden) nextButton.focus();
    });
    nextButton.addEventListener('click', () => {
        if (currentStep < steps.length - 1) {
            showStep(currentStep + 1);
            return;
        }
        dialog.close();
        sessionInput?.focus();
    });
    dialog.addEventListener('click', (event) => {
        if (event.target === dialog) dialog.close();
    });

    const failureDialog = document.getElementById('passwordLoginFailure');
    if (failureDialog) {
        function retryPassword() {
            failureDialog.close();
            document.getElementById('password')?.focus();
        }
        document.getElementById('passwordFailureClose').addEventListener('click', retryPassword);
        document.getElementById('passwordFailureRetry').addEventListener('click', retryPassword);
        document.getElementById('passwordFailureSession').addEventListener('click', () => {
            failureDialog.close();
            modeButton.click();
            if (!dialog.open) sessionInput?.focus();
        });
        failureDialog.addEventListener('click', (event) => {
            if (event.target === failureDialog) retryPassword();
        });
        failureDialog.showModal();
        document.getElementById('passwordFailureSession').focus();
    }
})();
