#!/usr/bin/env bash
# Classify a merge-group diff into buckets so ci.yml can skip the jobs a
# change cannot affect.
#
# Usage:
#   classify-changes.sh --enforce <true|false> --base <sha> --head <sha>
#   classify-changes.sh --enforce <true|false> --files <path>
#
# `--files` takes the output of `git diff --name-status --no-renames` and
# exists for the test script; the label lookup is skipped in that mode.
#
# Outputs, written to $GITHUB_OUTPUT (stdout when unset):
#   docs         a docs-only change is present
#   web_console  a js-packages change is present
#   all          run every job: some file fits no bucket, the event is not a
#                merge group, a PR in the group carries the `ci:full` label,
#                the lookup failed, or enforcement is off
#   enforced     whether skipping is enforced
#   reason       why `all` is true, for the summary
#
# Buckets:
#   docs         docs.feldera.com/**, docs/**, python/docs/**, and any *.md
#                that is not deleted. Deleting a *.md elsewhere may break a
#                Cargo.toml `readme` reference, so it falls through to `all`.
#   web_console  js-packages/** and the bun workspace files at the root.
#                The console is embedded into pipeline-manager, so this bucket
#                still builds Rust and the Docker image.
#
# Anything else, including openapi.json and .github/**, means `all`.
set -euo pipefail

enforce=false
base=""
head=""
files=""

while [ $# -gt 0 ]; do
  case "$1" in
    --enforce) enforce=$2; shift 2 ;;
    --base) base=$2; shift 2 ;;
    --head) head=$2; shift 2 ;;
    --files) files=$2; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

out() {
  printf '%s=%s\n' "$1" "$2" >> "${GITHUB_OUTPUT:-/dev/stdout}"
}

classify_file() {
  local status=$1 path=$2
  case "$path" in
    docs.feldera.com/*|docs/*|python/docs/*) echo docs; return ;;
    js-packages/*|package.json|bun.lock|bunfig.toml) echo web_console; return ;;
  esac
  if [[ "$path" == *.md && "$status" != D ]]; then
    echo docs
    return
  fi
  echo all
}

# GET a GitHub API path with the workflow token and print the response.
api() {
  curl -fsSL \
    -H "Authorization: Bearer ${GH_TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    "https://api.github.com/repos/${GITHUB_REPOSITORY}/$1"
}

# Returns 0 when any PR in base..head carries the `ci:full` label, 1 when none
# does, 2 when the lookup failed and the caller must fall back to `all`.
# Commit subjects carry no PR number here (rebase merges), so the PRs come
# from the commits-to-pulls endpoint.
has_full_label() {
  local commits numbers="" n labels
  [ -n "${GH_TOKEN:-}" ] || return 2
  command -v jq >/dev/null || return 2
  commits=$(git rev-list "$base..$head") || return 2
  for c in $commits; do
    n=$(api "commits/${c}/pulls" | jq -r '.[].number') || return 2
    numbers+="$n"$'\n'
  done
  for n in $(printf '%s' "$numbers" | sort -u); do
    labels=$(api "issues/${n}/labels" | jq -r '.[].name') || return 2
    grep -qx 'ci:full' <<< "$labels" && return 0
  done
  return 1
}

docs=false
web_console=false
all=false
reason=""
n_docs=0
n_web=0
n_all=0
unclassified=""

if [ -n "$files" ]; then
  diff=$(cat "$files")
elif [ "${GITHUB_EVENT_NAME:-merge_group}" != "merge_group" ]; then
  all=true
  reason="event is ${GITHUB_EVENT_NAME}, not merge_group"
  diff=""
elif [ -z "$base" ] || [ -z "$head" ]; then
  all=true
  reason="missing base or head sha"
  diff=""
elif ! diff=$(git diff --name-status --no-renames "$base" "$head"); then
  all=true
  reason="git diff ${base}..${head} failed"
  diff=""
fi

if [ -z "$diff" ] && [ "$all" = false ]; then
  all=true
  reason="empty diff"
fi

while IFS=$'\t' read -r status path; do
  [ -z "$path" ] && continue
  case "$(classify_file "$status" "$path")" in
    docs) docs=true; n_docs=$((n_docs + 1)) ;;
    web_console) web_console=true; n_web=$((n_web + 1)) ;;
    all)
      all=true
      n_all=$((n_all + 1))
      [ -z "$reason" ] && reason="files outside every bucket"
      [ $n_all -le 10 ] && unclassified+="  - \`${path}\`"$'\n'
      ;;
  esac
done <<< "$diff"

if [ "$all" = false ] && [ -z "$files" ]; then
  label_status=0
  has_full_label || label_status=$?
  case $label_status in
    0) all=true; reason="a PR in the merge group carries the ci:full label" ;;
    2) all=true; reason="label lookup failed" ;;
  esac
fi

would_all=$all
if [ "$all" = false ] && [ "$enforce" != true ]; then
  all=true
  reason="enforcement is off (CI_PATH_FILTER_ENFORCE)"
fi

out docs "$docs"
out web_console "$web_console"
out all "$all"
out enforced "$enforce"
out reason "$reason"

# Mirrors the `if:` conditions in ci.yml; keep the two in step.
jobs_kept="Build Docs"
if [ "$would_all" = true ]; then
  jobs_kept="everything"
elif [ "$web_console" = true ]; then
  jobs_kept="Web Console Unit Tests, Build Rust, Build Java, Build Docker, Web Console End-to-End Tests"
  [ "$docs" = true ] && jobs_kept+=", Build Docs"
fi

{
  echo "## Change classification"
  echo
  echo "| Bucket | Files |"
  echo "|---|---|"
  echo "| docs | ${n_docs} |"
  echo "| web_console | ${n_web} |"
  echo "| outside every bucket | ${n_all} |"
  echo
  if [ -n "$unclassified" ]; then
    echo "Files outside every bucket (first 10):"
    echo
    printf '%s' "$unclassified"
    echo
  fi
  if [ "$enforce" = true ]; then
    echo "Skipping is enforced. Jobs that run: ${jobs_kept}."
  else
    echo "Observe mode. Every job runs; with enforcement on, jobs that would run: ${jobs_kept}."
  fi
  if [ -n "$reason" ]; then
    echo
    echo "Reason for \`all\`: ${reason}."
  fi
} >> "${GITHUB_STEP_SUMMARY:-/dev/stderr}"
