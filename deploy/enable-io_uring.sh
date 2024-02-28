#! /bin/sh

if test "$1" = --help; then
   cat <<EOF
$0: modifies Docker seccomp policy to allow io_uring syscalls
usage: $0 POLICY
where POLICY is the name of an existing policy file, e.g. /usr/share/containers/seccomp.json

Prints the modified policy on stdout.
EOF
   exit 0
fi

if test $# != 1; then
    echo "$0: exactly one argument expected (use --help for help)"
    exit 1
fi

if test ! -e "$1"; then
    echo >&2 "$0: '$1' not found"
    exit 1
fi

if grep io_uring "$1" >/dev/null 2>&1; then
    echo >&2 "$0: warning: existing policy $1 mentions io_uring and might already allowlist it"
fi

if (jq --version) >/dev/null 2>&1; then : ; else
    echo >&2 "$0: required program 'jq' is not installed"
    exit 1
fi

jq < "$1" '
    .syscalls |= [
      {
	"names": [
	  "io_uring_enter",
	  "io_uring_register",
	  "io_uring_setup"
	],
	"action": "SCMP_ACT_ALLOW",
	"args": [],
	"comment": "",
	"includes": {},
	"excludes": {}
      }
    ] + .
'
