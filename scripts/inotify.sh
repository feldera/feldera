#!/usr/bin/env bash
# Inspect and manage inotify watch usage.
#
# Usage:
#   scripts/inotify.sh          Show per-process watch counts
#   scripts/inotify.sh --free   Kill the top node.js watcher to free watches

set -euo pipefail

MIN_WATCHES_TO_KILL=10000

# Watch counts per PID, summed over every inotify fd the process holds.
declare -A watches=()
while read -r fd_dir fd; do
  pid=${fd_dir#/proc/}
  pid=${pid%/fd}
  n=$(grep -c '^inotify' "/proc/$pid/fdinfo/$fd" 2>/dev/null) || n=0
  watches[$pid]=$((${watches[$pid]:-0} + n))
done < <(find /proc/[0-9]*/fd -lname 'anon_inode:inotify' -printf '%h %f\n' 2>/dev/null)

limit=$(cat /proc/sys/fs/inotify/max_user_watches)
used=0
for pid in "${!watches[@]}"; do
  if [ "${watches[$pid]}" -eq 0 ]; then
    unset "watches[$pid]"
  else
    used=$((used + watches[$pid]))
  fi
done

# "count pid" lines, highest count first.
watchers_by_count() {
  for pid in "${!watches[@]}"; do
    echo "${watches[$pid]} $pid"
  done | sort -rn
}

argv0() {
  tr '\0' '\n' < "/proc/$1/cmdline" 2>/dev/null | head -1
}

# node renames its main thread, so /proc/PID/comm cannot identify an interpreter.
is_node() {
  case "$(basename -- "$(argv0 "$1")")" in
    node | node[0-9]* | nodejs) return 0 ;;
    *) return 1 ;;
  esac
}

# Command line with the interpreter reduced to its name and long paths collapsed
# to their last two components, so the distinguishing arguments survive truncation.
short_cmd() {
  local pid=$1 arg out="" first=1
  while IFS= read -r -d '' arg; do
    if [ "$first" = 1 ]; then
      first=0
      arg=$(basename -- "$arg")
    else
      case "$arg" in
        /*/*/*) arg=".../$(basename -- "$(dirname -- "$arg")")/$(basename -- "$arg")" ;;
      esac
    fi
    out+="$arg "
  done < "/proc/$pid/cmdline"
  echo "${out% }" | cut -c1-110
}

show_status() {
  echo "inotify watches: $used / $limit"
  echo ""
  watchers_by_count | while read -r count pid; do
    printf "%6d  PID %-8s %s\n" "$count" "$pid" "$(short_cmd "$pid")"
  done
}

free_watches() {
  if [ ${#watches[@]} -eq 0 ]; then
    echo "No inotify watchers found."
    exit 0
  fi

  # Walk watchers from the heaviest down to the first one we can kill.
  while read -r count pid; do
    if ! is_node "$pid"; then
      printf "Skipping PID %s (%d watches), not a node process: %s\n" \
        "$pid" "$count" "$(short_cmd "$pid")"
      continue
    fi
    if [ "$count" -lt "$MIN_WATCHES_TO_KILL" ]; then
      echo "Heaviest node watcher holds only $count watches, nothing worth killing."
      exit 0
    fi

    echo "Top node watcher: PID $pid using $count of $limit watches"
    echo "  $(short_cmd "$pid")"
    echo "Killing PID $pid to free $count watches..."
    kill "$pid"
    echo "Done. Freed ~$count inotify watches."
    exit 0
  done < <(watchers_by_count)

  echo "No node watcher found to kill."
  exit 1
}

case "${1:-}" in
  --free) free_watches ;;
  "")     show_status ;;
  *)      echo "Usage: $0 [--free]"; exit 1 ;;
esac
