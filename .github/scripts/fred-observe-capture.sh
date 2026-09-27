#!/usr/bin/env bash
#
# Captures what a failed Merge Queue Tasks run left behind, as the first step
# of fred-observe's triage job, so Claude starts from a consistent picture
# instead of spending turns collecting one, and so a human reading the run
# later has the same picture from the uploaded archive.
#
# Exits 0 whatever it finds; the verdict rides GITHUB_OUTPUT:
#
#   reason         jobs-failed, or unclear when no watched job reported a
#                  failure: a run the merge queue cancelled while rebuilding,
#                  or a build failure, which is the author's to read
#   failure_count  watched jobs that reported failure or timed_out
#   pr_number      the pull request the merge queue was testing, or empty
#
# Environment:
#   GH_TOKEN              token with actions:read on the repository
#   GITHUB_REPOSITORY     owner/repo
#   OBSERVED_RUN_ID       the run whose jobs to report on
#   OBSERVED_HEAD_BRANCH  the run's head branch, gh-readonly-queue/main/pr-N-<sha>
#   OBSERVE_WATCH_JOBS    newline-separated names of the reusable workflows
#                         whose jobs count ("Unit Tests", "Integration Tests")
#   OBSERVE_DIR           output directory (default /tmp/fred-observe)

set -uo pipefail
export LC_ALL=C

OBSERVE_DIR="${OBSERVE_DIR:-/tmp/fred-observe}"

ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { echo "[$(ts)] $*"; }

# Lines worth reading first in a CI log: runner annotations, Rust and Python
# failures, and the libtest/pytest summaries.
MARKER_REGEX='##\[error\]|panicked at|thread .* panicked|test result: FAILED|^.{0,40}FAILED |[A-Za-z]+Error[:.]|Traceback \(most recent call last\)|^.{0,40}E  +|error\[E[0-9]+\]|exit code [1-9]|timed out'

capture_jobs() {
  local watch_regex="$1" jobs watched failures
  jobs=$(gh api --paginate "repos/$GITHUB_REPOSITORY/actions/runs/$OBSERVED_RUN_ID/jobs?per_page=100" \
    --jq '.jobs[] | {name, status, conclusion, id, html_url, started_at, completed_at}' 2>/dev/null |
    jq -s '.' 2>/dev/null)
  # the cancel sentinels carry the watched workflow's prefix too and never fail on their own
  watched=$(jq --arg re "$watch_regex" \
    '[.[] | select((.name | test($re)) and (.name | test("Cancel if") | not))]' <<<"$jobs" 2>/dev/null)
  failures=$(jq '[.[] | select(.conclusion == "failure" or .conclusion == "timed_out")]' <<<"$watched" 2>/dev/null)

  jq '.' <<<"$watched" >"$OBSERVE_DIR/jobs.json"
  jq '.' <<<"$failures" >"$OBSERVE_DIR/failed-jobs.json"
  jq -r '.[] | "\(.id)\t\(.conclusion)\t\(.name)\t\(.html_url)"' <<<"$failures" >"$OBSERVE_DIR/failed-jobs.txt"
  jq -r '.[] | "  \(.name): \(.conclusion // .status)"' <<<"$watched"

  local failure_count reason
  failure_count=$(jq 'length' <<<"$failures")
  if [ "$failure_count" -gt 0 ]; then
    reason="jobs-failed"
  else
    reason="unclear"
  fi
  say "$failure_count watched jobs failed (reason=$reason)"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    {
      echo "reason=$reason"
      echo "failure_count=$failure_count"
    } >>"$GITHUB_OUTPUT"
  fi
}

# One log per failed job plus a line-numbered index of its error markers, so
# the first read is the markers and not 140 KB of runner chatter.
capture_logs() {
  local job_id
  mkdir -p "$OBSERVE_DIR/logs"
  while read -r job_id; do
    [ -z "$job_id" ] && continue
    say "downloading log of job $job_id"
    # the API follows the redirect to blob storage; logs carry terminal colors
    gh api --allow-escape-sequences "repos/$GITHUB_REPOSITORY/actions/jobs/$job_id/logs" 2>/dev/null |
      sed 's/\x1b\[[0-9;]*[A-Za-z]//g' >"$OBSERVE_DIR/logs/$job_id.log"
    grep -n -E "$MARKER_REGEX" "$OBSERVE_DIR/logs/$job_id.log" 2>/dev/null |
      head -300 >"$OBSERVE_DIR/logs/$job_id-markers.txt"
    say "  $(wc -l <"$OBSERVE_DIR/logs/$job_id.log" | tr -d ' ') lines, $(wc -l <"$OBSERVE_DIR/logs/$job_id-markers.txt" | tr -d ' ') markers"
  done < <(jq -r '.[].id' "$OBSERVE_DIR/failed-jobs.json")
}

# Merge queue refs look like gh-readonly-queue/main/pr-1888-<sha>; a direct
# dispatch of ci.yml has no PR and nothing to report to.
find_pr() {
  local pr_number=""
  if [[ "$OBSERVED_HEAD_BRANCH" =~ /pr-([0-9]+)- ]]; then
    pr_number="${BASH_REMATCH[1]}"
    say "run tested PR #$pr_number"
  else
    say "no pull request in $OBSERVED_HEAD_BRANCH"
  fi
  echo "$pr_number" >"$OBSERVE_DIR/pr-number.txt"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "pr_number=$pr_number" >>"$GITHUB_OUTPUT"
  fi
}

main() {
  : "${GITHUB_REPOSITORY:?}" "${OBSERVED_RUN_ID:?}" "${OBSERVED_HEAD_BRANCH:?}" "${OBSERVE_WATCH_JOBS:?}"
  mkdir -p "$OBSERVE_DIR"

  # ci.yml calls each test workflow, so job names read "Unit Tests / Rust Unit Tests (...)"
  local alternatives
  alternatives=$(printf '%s' "$OBSERVE_WATCH_JOBS" | grep -v '^[[:space:]]*$' | paste -sd'|' -)
  capture_jobs "^($alternatives) / "
  capture_logs
  find_pr

  say "capture complete: $(du -sh "$OBSERVE_DIR" 2>/dev/null | cut -f1) in $OBSERVE_DIR"
}

main "$@"
