# Visual note pipeline rollback

The visual note pipeline is additive. It stores `visual_regions` inside the
existing `source_transcription` JSON and `visual_refs` inside card JSON. It does
not add or migrate database tables, and older code ignores these extra keys.

## Immediate disable

Set this environment variable and restart the service:

```text
E3_VISUAL_NOTE_PIPELINE=0
```

New uploads then use the previous text-only transcription and card contracts.
Set it back to `1` to re-enable visual processing.

## Complete code rollback

The exact pre-feature commit is preserved by both refs:

```text
backup/pre-visual-note-pipeline-20260825
pre-visual-note-pipeline-20260825
```

The visual feature is kept in one isolated commit. Reverting that commit
restores the previous code while preserving later unrelated work:

```powershell
git revert <visual-feature-commit>
```

No data rollback is required. Existing notes remain readable because the
additional JSON fields are optional and do not change the database schema.
