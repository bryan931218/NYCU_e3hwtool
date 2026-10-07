(() => {
    const filter = document.getElementById('sourceFilter');
    filter?.addEventListener('change', () => {
        let count = 0;
        document.querySelectorAll('#sourceVisits tr').forEach(row => {
            row.hidden = Boolean(filter.value && row.dataset.source !== filter.value);
            if (!row.hidden) count++;
        });
        document.getElementById('sourceFilterEmpty').hidden = count > 0;
    });
    const channel = document.getElementById('sourceShareChannel');
    const link = document.getElementById('sourceShareLink');
    const status = document.getElementById('sourceShareStatus');
    if (!channel || !link) return;
    function updateLink() {
        const url = new URL(channel.closest('[data-share-base]').dataset.shareBase);
        url.searchParams.set('utm_source', channel.value);
        link.value = url.href;
        status.textContent = '';
    }
    channel.addEventListener('change', updateLink);
    updateLink();
    document.getElementById('sourceShareCopy').addEventListener('click', async () => {
        try {
            await navigator.clipboard.writeText(link.value);
            status.textContent = '已複製';
        } catch (_) {
            link.focus(); link.select();
            status.textContent = '請複製已選取的連結';
        }
    });
})();
