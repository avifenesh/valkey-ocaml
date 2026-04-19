#!/usr/bin/env bash
# Point git at the tracked hooks directory so pre-push (and any
# future hook) is version-controlled with the repo rather than
# living in each contributor's .git/hooks.
#
# Safe to re-run. To undo: git config --unset core.hooksPath

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

chmod +x scripts/git-hooks/pre-push

git config core.hooksPath scripts/git-hooks

echo "installed: git config core.hooksPath = scripts/git-hooks"
echo "active hooks:"
ls -l scripts/git-hooks/
