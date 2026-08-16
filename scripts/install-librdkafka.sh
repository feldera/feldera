#!/usr/bin/env bash
# Builds librdkafka against AWS-LC and installs both under $PREFIX.
#
# rdkafka-sys otherwise compiles a vendored librdkafka against whatever OpenSSL
# pkg-config finds, which puts Kafka TLS on a second cryptographic
# implementation. Building it here instead lets us point it at AWS-LC, and the
# `dynamic-linking` feature then makes rdkafka-sys consume this install rather
# than compile its own copy.
#
# The container images and CI run this script, and so should developers: the
# `dynamic-linking` feature is unconditional, so a build finds librdkafka here
# or not at all. Ubuntu's package is far older than the version rdkafka-sys
# requires, which is why this builds from source rather than calling apt.
#
# The librdkafka and AWS-LC versions are derived from the lockfile, so neither
# can drift from what the crates expect: rdkafka-sys names its vendored version
# in its own, as `4.10.0+2.12.1`, and aws-lc-sys vendors one AWS-LC release.
set -euo pipefail

PREFIX="${PREFIX:-/usr/local}"
# Switching to the FIPS-validated module means building AWS-LC with -DFIPS=1
# and moving aws-lc-rs to aws-lc-fips-sys in the same change. Doing one without
# the other leaves two AWS-LC copies in the binary; validate-crypto-deps.sh
# fails when that happens.
AWS_LC_FIPS="${AWS_LC_FIPS:-0}"

script_path="${BASH_SOURCE[0]:-$0}"
repo_root=$(cd "$(dirname "$script_path")/.." 2>/dev/null && pwd) || repo_root="."
lockfile="${CARGO_LOCK:-$repo_root/Cargo.lock}"

if [ -n "${LIBRDKAFKA_REF:-}" ]; then
    librdkafka_version="${LIBRDKAFKA_REF#v}"
elif [ -f "$lockfile" ]; then
    # rdkafka-sys version is `<crate version>+<librdkafka version>`.
    librdkafka_version=$(grep -A1 'name = "rdkafka-sys"' "$lockfile" |
        grep '^version' | sed 's/.*+//; s/"//')
else
    echo "error: cannot determine the librdkafka version." >&2
    echo "Point CARGO_LOCK at a lockfile, or set LIBRDKAFKA_REF explicitly." >&2
    exit 1
fi

if [ -z "$librdkafka_version" ]; then
    echo "error: no rdkafka-sys entry in $lockfile." >&2
    exit 1
fi

# Checked-in source identity per librdkafka version. A version bump in
# rdkafka-sys fails here until the new upstream tag is reviewed and its
# commit added.
librdkafka_commit="${LIBRDKAFKA_COMMIT:-}"
if [ -z "$librdkafka_commit" ]; then
    case "$librdkafka_version" in
        2.12.1) librdkafka_commit="e1db7eaa517f0a6438bc846a9c49ede73b9ea211" ;;
        *)
            echo "error: no pinned commit for librdkafka v$librdkafka_version." >&2
            echo "Review the upstream tag and add its commit OID to this script," >&2
            echo "or set LIBRDKAFKA_COMMIT explicitly." >&2
            exit 1
            ;;
    esac
fi

# Checked-in source identity per aws-lc-sys version: the commit behind the
# AWS-LC release tag that crate vendors (its include/openssl/base.h names the
# release), so the librdkafka in the process carries the same AWS-LC as the
# Rust side. Fetching by commit OID rather than by tag means a retargeted
# upstream ref cannot change what gets built. An aws-lc-sys bump in the
# lockfile fails here until the new release is reviewed and its commit added.
aws_lc_commit="${AWS_LC_COMMIT:-}"
if [ -z "$aws_lc_commit" ]; then
    aws_lc_sys_version=""
    if [ -f "$lockfile" ]; then
        aws_lc_sys_version=$(grep -A1 'name = "aws-lc-sys"' "$lockfile" |
            grep '^version' | sed 's/version = //; s/"//g')
    fi
    case "$aws_lc_sys_version" in
        # AWS-LC v5.5.0
        0.44.0) aws_lc_commit="991e67ff4cf04df4dd89e407f8b920c6936cb56a" ;;
        *)
            echo "error: no pinned AWS-LC commit for aws-lc-sys ${aws_lc_sys_version:-<none in $lockfile>}." >&2
            echo "Review the AWS-LC release that aws-lc-sys vendors and add its" >&2
            echo "commit OID to this script, or set AWS_LC_COMMIT explicitly." >&2
            exit 1
            ;;
    esac
fi

echo "librdkafka v$librdkafka_version ($librdkafka_commit), AWS-LC $aws_lc_commit, prefix $PREFIX"

for tool in cmake git awk go make cc; do
    command -v "$tool" >/dev/null || { echo "error: $tool is required." >&2; exit 1; }
done

jobs="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Fetches `commit` from `url` into `dest` and fails closed on any mismatch,
# so nothing from an upstream tree runs unless it is exactly the pinned OID.
fetch_pinned() {
    url="$1"
    commit="$2"
    dest="$3"
    git init -q "$dest"
    git -C "$dest" fetch -q --depth 1 "$url" "$commit"
    git -C "$dest" checkout -q --detach FETCH_HEAD
    resolved=$(git -C "$dest" rev-parse HEAD)
    if [ "$resolved" != "$commit" ]; then
        echo "error: $url delivered $resolved, expected $commit." >&2
        exit 1
    fi
}

# AWS-LC. BUILD_LIBSSL is off by default and librdkafka links libssl, so the
# default build is not enough. The symbol prefix stays: aws-lc-rs links its own
# copy into the same binary, and the prefix is what keeps the two apart.
fetch_pinned https://github.com/aws/aws-lc.git "$aws_lc_commit" "$work/aws-lc"
cmake -S "$work/aws-lc" -B "$work/aws-lc-build" \
    -DCMAKE_BUILD_TYPE=Release \
    "-DCMAKE_C_FLAGS=-fPIC -w" \
    -DCMAKE_INSTALL_PREFIX="$work/aws-lc-install" \
    -DBUILD_LIBSSL=ON \
    -DBUILD_SHARED_LIBS=OFF \
    -DBUILD_TESTING=OFF \
    -DBUILD_TOOL=OFF \
    -DFIPS="$AWS_LC_FIPS" >"$work/aws-lc-configure.log" 2>&1
if ! cmake --build "$work/aws-lc-build" --parallel "$jobs" >"$work/aws-lc-build.log" 2>&1; then
    echo "error: AWS-LC build failed." >&2
    tail -40 "$work/aws-lc-build.log" >&2
    exit 1
fi
cmake --install "$work/aws-lc-build" >"$work/aws-lc-install.log" 2>&1

aws_lc_lib=$(dirname "$(find "$work/aws-lc-install" -name libcrypto.a -print -quit)")
aws_lc_include="$work/aws-lc-install/include"
for lib in libcrypto.a libssl.a; do
    [ -f "$aws_lc_lib/$lib" ] || { echo "error: AWS-LC did not produce $lib." >&2; exit 1; }
done

# librdkafka.
fetch_pinned https://github.com/confluentinc/librdkafka.git "$librdkafka_commit" "$work/librdkafka"
cd "$work/librdkafka"

# rdkafka_ssl.c calls HMAC() without including <openssl/hmac.h>. OpenSSL
# supplies the declaration transitively through x509.h and AWS-LC does not, so
# the call compiles as an implicit declaration returning int, truncating the
# returned pointer. Fixed upstream in confluentinc/librdkafka#5552, unmerged;
# drop this once a release carries it.
if ! grep -q "openssl/hmac.h" src/rdkafka_ssl.c; then
    awk '/^#include <openssl\/x509\.h>$/ && !inserted {
             print "#include <openssl/hmac.h>"; inserted = 1
         }
         { print }' src/rdkafka_ssl.c > src/rdkafka_ssl.c.patched
    mv src/rdkafka_ssl.c.patched src/rdkafka_ssl.c
    grep -q "openssl/hmac.h" src/rdkafka_ssl.c || {
        echo "error: could not apply the hmac.h patch; check whether upstream restructured the includes." >&2
        exit 1
    }
fi

# These flags mirror the cargo features rdkafka-sys used to build with, so the
# connectors keep the same capabilities.
CPPFLAGS="-I$aws_lc_include" \
CFLAGS="-Werror=implicit-function-declaration" \
LDFLAGS="-L$aws_lc_lib" \
PKG_CONFIG_PATH="$aws_lc_lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}" \
./configure --prefix="$PREFIX" \
    --enable-ssl \
    --enable-gssapi \
    --enable-zlib \
    --enable-zstd \
    --disable-curl \
    --disable-lz4-ext >"$work/configure.log" 2>&1 || {
        echo "error: librdkafka configure failed." >&2
        tail -30 "$work/configure.log" >&2
        exit 1
    }

for want in WITH_SSL WITH_SASL_CYRUS WITH_ZLIB WITH_ZSTD; do
    grep -q "^${want}=[[:space:]]*y$" Makefile.config || {
        echo "error: librdkafka configure did not enable $want." >&2
        echo "Its development package is probably missing; see Makefile.config." >&2
        exit 1
    }
done

if ! make -j"$jobs" libs >"$work/build.log" 2>&1; then
    echo "error: librdkafka build failed." >&2
    tail -40 "$work/build.log" >&2
    exit 1
fi
if ! make install >"$work/install.log" 2>&1; then
    echo "error: librdkafka install to $PREFIX failed." >&2
    tail -20 "$work/install.log" >&2
    exit 1
fi

# librdkafka must carry AWS-LC inside rather than link a system OpenSSL. A
# NEEDED entry for libssl or libcrypto means configure found one and preferred
# it, which silently reintroduces the implementation this script exists to
# avoid.
shared=""
for candidate in "$PREFIX/lib/librdkafka.so.1" "$PREFIX/lib/librdkafka.1.dylib"; do
    if [ -f "$candidate" ]; then
        shared="$candidate"
        break
    fi
done
if [ -n "$shared" ]; then
    if command -v readelf >/dev/null; then
        if readelf -d "$shared" | grep -qE 'Shared library: \[lib(ssl|crypto)\.so'; then
            echo "error: librdkafka links a shared OpenSSL:" >&2
            readelf -d "$shared" | grep -E 'Shared library: \[lib(ssl|crypto)' >&2
            exit 1
        fi
    elif command -v otool >/dev/null; then
        if otool -L "$shared" | grep -qE 'lib(ssl|crypto)[.0-9]*\.dylib'; then
            echo "error: librdkafka links a shared OpenSSL:" >&2
            otool -L "$shared" | grep -E 'lib(ssl|crypto)' >&2
            exit 1
        fi
    fi
fi

command -v ldconfig >/dev/null && ldconfig 2>/dev/null || true

echo "installed librdkafka $librdkafka_version to $PREFIX"
