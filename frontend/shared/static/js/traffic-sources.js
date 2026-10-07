(() => {
    const filter = document.getElementById('sourceFilter');
    if (!filter) return;
    filter.addEventListener('change', () => {
        let count = 0;
        document.querySelectorAll('#sourceVisits tr').forEach(row => {
            row.hidden = Boolean(filter.value && row.dataset.source !== filter.value);
            if (!row.hidden) count++;
        });
        document.getElementById('sourceFilterEmpty').hidden = count > 0;
    });
})();
