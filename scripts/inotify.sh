#!/usr/bin/env bash
# Inspect and manage inotify watch usage.
#
# Usage:
#   scripts/inotify.sh                    Show per-process watch counts
#   scripts/inotify.sh --free             Kill the VS Code file watcher
#   scripts/inotify.sh --free --dry-run   Report what --free would kill
#   scripts/inotify.sh --free --yes       Kill a non-watcher node process unprompted

set -euo pipefail

MIN_WATCHES_TO_KILL=10000

# Watch counts per PID, summed over every inotify fd the process holds.
declare -A watches=()
unreadable=0
while read -r fd_dir fd; do
  pid=${fd_dir#/proc/}
  pid=${pid%/fd}
  if [ -r "/proc/$pid/fdinfo/$fd" ]; then
    n=$(grep -c '^inotify' "/proc/$pid/fdinfo/$fd" 2>/dev/null) || n=0
  else
    n=0
    unreadable=$((unreadable + 1))
  fi
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
  done 2>/dev/null < "/proc/$pid/cmdline" || true
  if [ -z "$out" ]; then
    if [ -e "/proc/$pid" ]; then
      out="[$(cat "/proc/$pid/comm" 2>/dev/null || echo unknown)]"
    else
      out="(exited)"
    fi
  fi
  # An argv may contain newlines, which would break the table and print past the
  # width limit, so flatten it before truncating.
  out=${out% }
  printf '%.110s\n' "${out//$'\n'/ }"
}

# VS Code forks one process per watcher type; only the file watcher is safe to
# kill, because the server respawns it.
is_file_watcher() {
  local cmdline
  cmdline=$(tr '\0' ' ' < "/proc/$1/cmdline" 2>/dev/null) || return 1
  case "$cmdline" in
    *--type=fileWatcher* | *watcherMain*) return 0 ;;
    *) return 1 ;;
  esac
}

show_status() {
  echo "inotify watches: $used / $limit"
  echo ""
  watchers_by_count | while read -r count pid; do
    printf "%6d  PID %-8s %s\n" "$count" "$pid" "$(short_cmd "$pid")"
  done
  echo ""
  echo "Counts cover processes owned by $(id -un) only; the limit is per user."
  if [ "$unreadable" -gt 0 ]; then
    echo "$unreadable inotify fds were unreadable and counted as 0."
  fi
}

# Kill $1, or report the kill under --dry-run.
kill_watcher() {
  local pid=$1 count=$2
  if [ "$dry_run" = 1 ]; then
    echo "Would kill PID $pid to free $count watches (--dry-run)."
    exit 0
  fi
  echo "Killing PID $pid to free $count watches..."
  if ! kill "$pid" 2>/dev/null; then
    echo "PID $pid is already gone; its watches are free."
    exit 0
  fi
  echo "Done. Freed ~$count inotify watches."
  exit 0
}

# A node process other than the file watcher may be the editor server itself, so
# killing it drops the session. Require the user to say so.
confirm_kill() {
  local pid=$1 reply
  if [ "$assume_yes" = 1 ]; then
    return 0
  fi
  # The caller's stdin is the watcher list, so ask the terminal directly.
  if ! { exec 3< /dev/tty; } 2>/dev/null; then
    echo "PID $pid is not the VS Code file watcher; killing it may end your session."
    echo "Re-run with --yes to kill it anyway, or --dry-run to see this without killing."
    exit 1
  fi
  read -r -u 3 -p "Kill PID $pid? Killing it may end your session. [y/N] " reply
  exec 3<&-
  [[ "$reply" =~ ^[Yy]$ ]]
}

free_watches() {
  if [ ${#watches[@]} -eq 0 ]; then
    echo "No inotify watchers found."
    exit 0
  fi

  # Walk watchers from the heaviest down to the first one worth killing.
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

    if is_file_watcher "$pid"; then
      kill_watcher "$pid" "$count"
    fi
    if confirm_kill "$pid"; then
      kill_watcher "$pid" "$count"
    fi
    echo "Left PID $pid alone."
    exit 0
  done < <(watchers_by_count)

  echo "No node watcher found to kill."
  exit 1
}

free=0
dry_run=0
assume_yes=0
for arg in "$@"; do
  case "$arg" in
    --free)    free=1 ;;
    --dry-run) dry_run=1 ;;
    --yes)     assume_yes=1 ;;
    *)         echo "Usage: $0 [--free [--dry-run] [--yes]]"; exit 1 ;;
  esac
done

if [ "$free" = 1 ]; then
  free_watches
elif [ "$dry_run" = 1 ] || [ "$assume_yes" = 1 ]; then
  echo "--dry-run and --yes apply to --free only."
  exit 1
else
  show_status
fi
