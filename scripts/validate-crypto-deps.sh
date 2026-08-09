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
#   openssl-sys  the FFI layer beneath it. Kafka used to require it, because
#                rdkafka's `ssl` feature depends on it and compiled a vendored
#                librdkafka against whatever OpenSSL pkg-config found. rdkafka
#                now uses `dynamic-linking` against the librdkafka that
#                scripts/install-librdkafka.sh builds against AWS-LC, so
#                nothing needs this crate.
#   native-tls   how openssl usually arrives; on macOS and Windows it binds
#                Security.framework and schannel instead, so checking for
#                openssl alone would miss a second TLS stack on those platforms
#   boring       BoringSSL wrapper, same symbol-collision class
#
# Allowed:
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

blocked=$(echo "$packages" | grep -E "^(openssl|openssl-sys|native-tls|boring|boring-sys) v" || true)
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
    echo
    echo 'openssl-sys specifically arrives through rdkafka. Kafka does not need it:'
    echo 'keep rdkafka on "dynamic-linking" rather than "ssl", and build librdkafka'
    echo 'with scripts/install-librdkafka.sh.'
    exit 1
fi

# One AWS-LC, not two. aws-lc-sys and aws-lc-fips-sys are separate crates, so
# cargo cannot unify them; a tree holding both compiles and links the whole
# library twice. Adopting the FIPS module means moving aws-lc-rs to the fips
# feature and building AWS-LC with -DFIPS=1 in scripts/install-librdkafka.sh at
# the same time, so that librdkafka and the Rust side agree.
backends=$(echo "$packages" | grep -E "^aws-lc(-fips)?-sys v" | awk '{print $1}' | sort -u)
if [ "$(echo "$backends" | grep -c .)" -gt 1 ]; then
    echo "error: two AWS-LC implementations are in the dependency tree:"
    echo "$backends" | sed 's/^/  /'
    echo
    echo "Both are compiled and linked, so the binary carries the library twice."
    echo "Their symbol prefixes differ, which is why nothing collides and the"
    echo "waste stays invisible at run time. Make both ends agree:"
    echo
    echo "  cargo tree -i aws-lc-sys --target all"
    echo "  cargo tree -i aws-lc-fips-sys --target all"
    exit 1
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
