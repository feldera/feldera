#!/bin/bash

set -e

REPO_ROOT=$(git rev-parse --show-toplevel)

# Context files: CLAUDE.md anywhere, plus everything under .claude/skills/
CONTEXT_PATTERN='(^|/)CLAUDE\.md$|^\.claude/skills/'

list_branch_context_files() {
  git ls-tree -r --name-only origin/claude-context | grep -E "$CONTEXT_PATTERN"
}

pull_claude_context() {
  FILES=$(list_branch_context_files)
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
    # Compare blobs directly. The pulled files are untracked, so git diff reports them as deleted.
    if [ -f "$REPO_ROOT/$f" ] && ! git show "origin/claude-context:$f" | cmp -s - "$REPO_ROOT/$f"; then
      CHANGED_FILES+=("$f")
    fi
  done < <(list_branch_context_files)

  # New context files not yet on claude-context
  while IFS= read -r f; do
    rel="${f#$REPO_ROOT/}"
    if ! git ls-tree origin/claude-context -- "$rel" 2>/dev/null | grep -q .; then
      CHANGED_FILES+=("$rel")
    fi
  done < <( (find "$REPO_ROOT" -name 'CLAUDE.md' \
      -not -path '*/.git/*' \
      -not -path '*/node_modules/*'
    [ -d "$REPO_ROOT/.claude/skills" ] && find "$REPO_ROOT/.claude/skills" -type f) )

  if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
    echo "No changes to Claude context files compared to claude-context branch."
    return
  fi

  echo "Committing changes to Claude context files:"
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
      git add -f "${CHANGED_FILES[@]}" && \
      git commit --amend --no-edit --quiet)
    git push --force-with-lease origin "$(cd "$WORKTREE_DIR" && git rev-parse HEAD):refs/heads/claude-context"
  else
    (cd "$WORKTREE_DIR" && \
      git add -f "${CHANGED_FILES[@]}" && \
      git commit -m "Update Claude context files" --quiet)
    git push origin "$(cd "$WORKTREE_DIR" && git rev-parse HEAD):refs/heads/claude-context"
  fi
  echo "✅ Pushed ${#CHANGED_FILES[@]} context file(s) to claude-context branch."
}

case "$1" in
  ""|pull)
    pull_claude_context
    ;;
  push)
    push_claude_context "${2:-}"
    ;;
  *)
    echo "Usage: $0 [pull | push [--amend]]"
    echo "Deletions do not sync. Remove a file on the claude-context branch by hand."
    exit 1
    ;;
esac
