#!/usr/bin/env bash
# Inspect and manage inotify watch usage.
#
# Usage:
#   scripts/inotify.sh          Show per-process watch counts
#   scripts/inotify.sh --free   Kill the top node.js process to free watches

set -euo pipefail

# Collect per-process inotify watch counts into parallel arrays
pids=()
counts=()
for pid in $(find /proc/*/fd -lname 'anon_inode:inotify' 2>/dev/null | cut -d/ -f3 | sort -u); do
  total=0
  for fd in $(ls -la /proc/"$pid"/fd 2>/dev/null | grep inotify | awk '{print $9}'); do
    n=$(grep -c '^inotify' /proc/"$pid"/fdinfo/"$fd" 2>/dev/null) || true
    total=$((total + n))
  done
  if [ "$total" -gt 0 ]; then
    pids+=("$pid")
    counts+=("$total")
  fi
done

limit=$(cat /proc/sys/fs/inotify/max_user_watches)
used=0
for c in "${counts[@]}"; do used=$((used + c)); done

show_status() {
  echo "inotify watches: $used / $limit"
  echo ""
  # Sort by count descending
  for i in "${!pids[@]}"; do
    echo "${counts[$i]} ${pids[$i]}"
  done | sort -rn | while read -r count pid; do
    comm=$(cat /proc/"$pid"/comm 2>/dev/null || echo "unknown")
    cmd=$(tr '\0' ' ' < /proc/"$pid"/cmdline 2>/dev/null | cut -c1-120)
    printf "%6d  PID %-7s %-10s %s\n" "$count" "$pid" "($comm)" "$cmd"
  done
}

free_watches() {
  if [ ${#pids[@]} -eq 0 ]; then
    echo "No inotify watchers found."
    exit 0
  fi

  # Find top watcher
  top_idx=0
  for i in "${!counts[@]}"; do
    if [ "${counts[$i]}" -gt "${counts[$top_idx]}" ]; then
      top_idx=$i
    fi
  done
  top_pid=${pids[$top_idx]}
  top_count=${counts[$top_idx]}

  comm=$(cat /proc/"$top_pid"/comm 2>/dev/null || echo "unknown")
  cmd=$(tr '\0' ' ' < /proc/"$top_pid"/cmdline 2>/dev/null | cut -c1-120)

  echo "Top watcher: PID $top_pid ($comm) using $top_count of $limit watches"
  echo "  $cmd"

  if [[ "$comm" != "node" ]]; then
    echo "Not a node process — skipping kill."
    exit 1
  fi

  if [ "$top_count" -lt 10000 ]; then
    echo "Watch count is low ($top_count) — no need to kill."
    exit 0
  fi

  echo "Killing PID $top_pid to free $top_count watches..."
  kill "$top_pid"
  echo "Done. Freed ~$top_count inotify watches."
}

case "${1:-}" in
  --free) free_watches ;;
  "")     show_status ;;
  *)      echo "Usage: $0 [--free]"; exit 1 ;;
esac
