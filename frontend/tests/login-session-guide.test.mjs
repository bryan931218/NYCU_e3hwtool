import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../assignments/static/js/login-session-guide.js", import.meta.url), "utf8");
const seenKey = "e3_session_guide_seen_v1";

function element() {
    const listeners = new Map();
    return {
        hidden: false,
        style: {},
        textContent: "",
        focused: false,
        addEventListener(name, listener) { listeners.set(name, listener); },
        click() { listeners.get("click")?.({ target: this }); },
        focus() { this.focused = true; },
        dispatch(name, event) { listeners.get(name)?.(event); },
    };
}

function fixture({ stored = new Map(), blocked = false, passwordFailed = false } = {}) {
    const steps = Array.from({ length: 3 }, element);
    const scenes = Array.from({ length: 3 }, element);
    const dialog = {
        ...element(),
        open: false,
        openings: 0,
        querySelectorAll(selector) { return selector === "[data-guide-step]" ? steps : scenes; },
        showModal() { this.open = true; this.openings++; },
        close() { this.open = false; },
    };
    const mode = element();
    let active = false;
    mode.classList = { contains(name) { return name === "active" && active; } };
    const modeClick = mode.click.bind(mode);
    mode.click = () => { active = true; modeClick(); };
    const controls = {
        sessionHelpModal: dialog,
        sessionHelpButton: element(),
        sessionGuideClose: element(),
        sessionGuidePrev: element(),
        sessionGuideNext: element(),
        sessionGuideNextLabel: element(),
        sessionGuideCount: element(),
        sessionGuideProgress: element(),
        moodle_session: element(),
        password: element(),
    };
    if (passwordFailed) {
        controls.passwordLoginFailure = { ...element(), open: false,
            showModal() { this.open = true; }, close() { this.open = false; } };
        controls.passwordFailureClose = element();
        controls.passwordFailureRetry = element();
        controls.passwordFailureSession = element();
    }
    const document = {
        getElementById(id) { return controls[id]; },
        querySelector(selector) { return selector === '[data-mode="session"]' ? mode : null; },
    };
    const localStorage = {
        getItem(key) { if (blocked) throw new Error("storage blocked"); return stored.get(key) ?? null; },
        setItem(key, value) { if (blocked) throw new Error("storage blocked"); stored.set(key, value); },
    };
    vm.runInNewContext(source, { document, localStorage });
    return { dialog, mode, steps, scenes, stored, controls, isActive: () => active };
}

test("first Session switch opens the guide, then advances to the paste field", () => {
    const page = fixture();
    page.mode.click();
    assert.equal(page.dialog.openings, 1);
    assert.equal(page.stored.get(seenKey), "1");
    assert.equal(page.controls.sessionGuideCount.textContent, "01 / 03");
    assert.deepEqual(page.steps.map((step) => step.hidden), [false, true, true]);
    assert.equal(page.controls.sessionGuidePrev.hidden, true);
    page.controls.sessionGuideNext.click();
    assert.equal(page.controls.sessionGuideCount.textContent, "02 / 03");
    page.controls.sessionGuidePrev.click();
    assert.equal(page.controls.sessionGuideCount.textContent, "01 / 03");
    assert.equal(page.controls.sessionGuideNext.focused, true);
    page.controls.sessionGuideNext.click();
    page.controls.sessionGuideNext.click();
    assert.equal(page.controls.sessionGuideNextLabel.textContent, "開始貼上");
    assert.deepEqual(page.scenes.map((scene) => scene.hidden), [true, true, false]);
    page.controls.sessionGuideNext.click();
    assert.equal(page.dialog.open, false);
    assert.equal(page.controls.moodle_session.focused, true);
});

test("returning users are not interrupted, and the help button always reopens", () => {
    const stored = new Map([[seenKey, "1"]]);
    const page = fixture({ stored });
    page.mode.click();
    assert.equal(page.dialog.openings, 0);
    page.controls.sessionHelpButton.click();
    assert.equal(page.dialog.openings, 1);
    page.controls.sessionGuideClose.click();
    page.controls.sessionHelpButton.click();
    assert.equal(page.dialog.openings, 2);
    assert.equal(page.controls.sessionGuideCount.textContent, "01 / 03");
});

test("help selects Session mode and blocked storage does not break the tutorial", () => {
    const page = fixture({ blocked: true });
    page.controls.sessionHelpButton.click();
    assert.equal(page.isActive(), true);
    assert.equal(page.dialog.openings, 1);
    page.controls.sessionGuideClose.click();
    page.mode.click();
    assert.equal(page.dialog.openings, 1);
});

test("authentication failure opens a prompt and Session action opens first-time guidance", () => {
    const page = fixture({ passwordFailed: true });
    assert.equal(page.controls.passwordLoginFailure.open, true);
    assert.equal(page.controls.passwordFailureSession.focused, true);
    assert.equal(page.isActive(), false);
    page.controls.passwordFailureSession.click();
    assert.equal(page.controls.passwordLoginFailure.open, false);
    assert.equal(page.isActive(), true);
    assert.equal(page.dialog.open, true);
});

test("returning users switch directly to the Session field after authentication failure", () => {
    const page = fixture({ passwordFailed: true, stored: new Map([[seenKey, "1"]]) });
    page.controls.passwordFailureSession.click();
    assert.equal(page.dialog.open, false);
    assert.equal(page.isActive(), true);
    assert.equal(page.controls.moodle_session.focused, true);
});

test("retry dismisses the failure prompt without switching login mode", () => {
    const page = fixture({ passwordFailed: true });
    page.controls.passwordFailureRetry.click();
    assert.equal(page.controls.passwordLoginFailure.open, false);
    assert.equal(page.isActive(), false);
    assert.equal(page.controls.password.focused, true);
});
