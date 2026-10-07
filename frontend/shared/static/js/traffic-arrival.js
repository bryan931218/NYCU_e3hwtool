(() => {
    const script = document.currentScript;
    if (!script?.dataset.arrivalToken) return;
    let referrer = '';
    try {
        const url = new URL(document.referrer);
        if (['http:', 'https:'].includes(url.protocol)) referrer = url.origin;
        else if (url.protocol === 'android-app:') referrer = 'android-app://' + url.host;
    } catch (_) {}
    const navigation = performance.getEntriesByType?.('navigation')[0]?.type || 'navigate';
    fetch(script.dataset.arrivalUrl, {
        method: 'POST', credentials: 'same-origin', keepalive: true,
        headers: {'Content-Type':'application/json', 'X-CSRFToken': script.dataset.arrivalCsrf},
        body: JSON.stringify({token:script.dataset.arrivalToken, referrer, navigation}),
    }).catch(() => {});
})();
