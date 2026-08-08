#!/usr/bin/env bash
# Fails if a second TLS or cryptography implementation is in the dependency tree.
#
# Feldera routes cryptography through AWS-LC so that a single validated module
# serves the whole binary. A second implementation is a correctness problem, not
# only a compliance one: AWS-LC keeps OpenSSL's symbol names, so a binary
# containing both resolves each name to whichever archive the linker reaches
# first. Memory allocated by one library then gets freed by the other, which
# corrupts the heap and surfaces far from the cause.
#
# Blocked:
#   openssl      the crate that links a second implementation directly
#   native-tls   how openssl usually arrives; on macOS and Windows it binds
#                Security.framework and schannel instead, so checking for
#                openssl alone would miss a second TLS stack on those platforms
#   boring       BoringSSL wrapper, same symbol-collision class
#
# Allowed:
#   openssl-sys  the FFI layer, but only when it resolves to an AWS-LC backend,
#                which this script asserts; librdkafka links whatever it
#                resolves, so that pin is what keeps Kafka TLS on AWS-LC
#   aws-lc-sys   AWS-LC itself, symbol-prefixed by aws-lc-fips-sys/aws-lc-sys
#
# `ring` should also go: it is a second cryptographic implementation and is not
# FIPS-validated. The switch is not finished, so it cannot be blocked outright
# yet; what remains reaches it through crates pinned to object_store 0.13, which
# have to move together. Until then this ratchets, failing when a crate outside
# the known set starts pulling ring, so the remainder shrinks and never grows.
#
# This only resolves the lockfile, so it compiles nothing.
set -euo pipefail

# `(*)` marks a subtree cargo already printed; strip it so entries dedupe.
packages=$(cargo tree --target all --edges normal,build,dev --prefix none --format "{p}" 2>/dev/null |
    sed 's/ (\*)$//' | sort -u)

blocked=$(echo "$packages" | grep -E "^(openssl|native-tls|boring|boring-sys) v" || true)
if [ -n "$blocked" ]; then
    echo "error: a second TLS or cryptography implementation is in the dependency tree:"
    echo "$blocked" | sed 's/^/  /'
    echo
    echo "It is almost always pulled in by a dependency defaulting to native-tls."
    echo "Find who enables it with, for example:"
    echo
    echo "  cargo tree -e features -i native-tls --target all"
    echo
    echo "then set default-features = false on that dependency and select its rustls"
    echo "feature instead. Note that a workspace member cannot override"
    echo "default-features on an inherited dependency; change it at the workspace root."
    exit 1
fi

# openssl-sys is only safe while its backend is AWS-LC. Its aws-lc-fips and
# aws-lc features add aws-lc-fips-sys or aws-lc-sys as a direct dependency;
# without either it links the system OpenSSL, which is a second implementation
# wearing the allowed name.
if echo "$packages" | grep -qE "^openssl-sys v"; then
    backend=$(cargo tree -p openssl-sys --depth 1 --target all --prefix none --format "{p}" 2>/dev/null |
        grep -E "^aws-lc(-fips)?-sys v" || true)
    if [ -z "$backend" ]; then
        echo "error: openssl-sys is present but does not resolve to an AWS-LC backend."
        echo
        echo "It links the system OpenSSL, which reintroduces the implementation the"
        echo "rest of this check exists to keep out. Restore its backend feature:"
        echo
        echo '  openssl-sys = { version = "...", default-features = false, features = ["aws-lc-fips"] }'
        exit 1
    fi
fi

# Crates still known to pull ring. Shrink this list; do not extend it.
RING_ALLOWED="object_store parquet rustls rustls-webpki"

# `--depth 1` on the inverted tree lists ring plus exactly its direct parents.
ring_parents=$(cargo tree --invert ring --depth 1 --target all --edges normal,build,dev \
    --prefix none --format "{p}" 2>/dev/null |
    sed 's/ (\*)$//' | awk 'NF {print $1}' | grep -v "^ring$" | sort -u)

unexpected=""
for parent in $ring_parents; do
    case " $RING_ALLOWED " in
        *" $parent "*) ;;
        *) unexpected="$unexpected $parent" ;;
    esac
done

if [ -n "$unexpected" ]; then
    echo "error: a new dependency pulls in ring:"
    for parent in $unexpected; do echo "  $parent"; done
    echo
    echo "ring is a second cryptographic implementation and is not FIPS-validated."
    echo "Prefer the aws-lc-rs backend where a dependency offers the choice, usually"
    echo "a feature named aws-lc-rs or a -no-provider variant that defers to the"
    echo "process default. Check whether a newer release drops ring outright before"
    echo "assuming a fork is needed."
    echo
    echo "  cargo tree -e features -i ring --target all"
    exit 1
fi

if [ -n "$ring_parents" ]; then
    echo "note: ring is still reached through:" $ring_parents
    echo "It is not FIPS-validated and should go; these are pinned to object_store"
    echo "0.13 and have to move together."
fi
