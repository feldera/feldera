#!/bin/bash

set -e

REPO_ROOT=$(git rev-parse --show-toplevel)

pull_claude_context() {
  FILES=$(git ls-tree -r --name-only origin/claude-context | grep 'CLAUDE\.md')
  git checkout origin/claude-context -- $FILES
  git restore --staged $FILES
  echo "✅ Pulled Claude context files from claude-context branch."
}

push_claude_context() {
  local amend="${1:-}"
  git fetch origin claude-context --quiet

  BASE_COMMIT=$(git rev-parse origin/claude-context)
  CHANGED_FILES=()

  # Files already on claude-context that differ from working tree
  while IFS= read -r f; do
    if [ -f "$REPO_ROOT/$f" ] && ! git diff --quiet origin/claude-context -- "$f" 2>/dev/null; then
      CHANGED_FILES+=("$f")
    fi
  done < <(git ls-tree -r --name-only origin/claude-context | grep 'CLAUDE\.md')

  # New CLAUDE.md files not yet on claude-context
  while IFS= read -r f; do
    rel="${f#$REPO_ROOT/}"
    if ! git ls-tree origin/claude-context -- "$rel" 2>/dev/null | grep -q .; then
      CHANGED_FILES+=("$rel")
    fi
  done < <(find "$REPO_ROOT" -name 'CLAUDE.md' \
    -not -path '*/.git/*' \
    -not -path '*/node_modules/*')

  if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
    echo "No changes to CLAUDE.md files compared to claude-context branch."
    return
  fi

  echo "Committing changes to CLAUDE.md files:"
  printf '  %s\n' "${CHANGED_FILES[@]}"

  WORKTREE_DIR=$(mktemp -d)
  # shellcheck disable=SC2064
  trap "git worktree remove --force '$WORKTREE_DIR' 2>/dev/null; rm -rf '$WORKTREE_DIR'" EXIT

  git worktree add --detach --quiet "$WORKTREE_DIR" "$BASE_COMMIT"

  for f in "${CHANGED_FILES[@]}"; do
    mkdir -p "$WORKTREE_DIR/$(dirname "$f")"
    cp "$REPO_ROOT/$f" "$WORKTREE_DIR/$f"
  done

  if [ "$amend" = "--amend" ]; then
    (cd "$WORKTREE_DIR" && \
      git add "${CHANGED_FILES[@]}" && \
      git commit --amend --no-edit --quiet)
    git push --force-with-lease origin "$(cd "$WORKTREE_DIR" && git rev-parse HEAD):refs/heads/claude-context"
  else
    (cd "$WORKTREE_DIR" && \
      git add "${CHANGED_FILES[@]}" && \
      git commit -m "Update CLAUDE.md context files" --quiet)
    git push origin "$(cd "$WORKTREE_DIR" && git rev-parse HEAD):refs/heads/claude-context"
  fi
  echo "✅ Pushed ${#CHANGED_FILES[@]} CLAUDE.md file(s) to claude-context branch."
}

case "$1" in
  ""|pull)
    pull_claude_context
    ;;
  push)
    push_claude_context "${2:-}"
    ;;
  *)
    echo "Usage: $0 [push]"
    exit 1
    ;;
esac
