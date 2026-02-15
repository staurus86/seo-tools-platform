# Contributing Guide

Thanks for contributing.

## Workflow
1. Create a branch from `main`.
2. Keep changes focused and atomic.
3. Add/update tests for behavior changes.
4. Run local checks before opening PR.

## Local checks
```bash
python -m unittest
python scripts/site_pro_preflight.py
```

## Commit style
Use concise, descriptive commit messages.
Example:
- `Fix Site Audit Pro XLSX export when issue details are null`

## Pull Request requirements
- Explain what changed and why.
- Include test evidence.
- Mention risks and rollback plan for non-trivial changes.
- Add screenshots for UI changes.

## Coding notes
- Preserve UTF-8 encoding.
- Do not revert unrelated changes.
- Prefer small, reviewable diffs.
