#!/usr/bin/env bash
# Make src/splash/splash_logo.bin, the Feldera logo for the fda welcome animation.
#
# Input: Feldera-logo-black-2000px.png from the brand kit (2000x875, RGBA).
# Output: a label grid, one label per px. 0 = empty, 1 = bird body,
# 2 = far wing, 3 = wordmark. Format: u16 width, u16 height, then runs of
# (label u8, length u16), all little endian.
#
# Needs ImageMagick 7 (`magick`), od, awk and xxd.
#
# Usage: make_splash_logo.sh <logo.png> <out.bin>
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <logo.png> <out.bin>" >&2
    exit 2
fi
src=$1
dst=$2
command -v magick >/dev/null || { echo "$0: needs ImageMagick 7 (magick)" >&2; exit 1; }

# The bird sits top right, clear of the letters.
bird_region="744x501+1256+0"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

size=$(magick identify -format '%wx%h' "$src")
if [[ $size != "2000x875" ]]; then
    echo "$0: expected a 2000x875 logo, got $size" >&2
    exit 1
fi

# Logo shape: white where alpha > 50%.
magick "$src" -alpha extract -threshold 50% "$tmp/mask.png"

# The far wing is the only shape right of x=1930. Its first px seeds the fill.
seed=$(magick "$tmp/mask.png" -crop 70x875+1930+0 +repage txt:- |
    awk -F'[,:]' '!found && /gray\(255\)/ { print $1 + 1930 "," $2; found = 1 }')
[[ -n $seed ]] || { echo "$0: far wing not found" >&2; exit 1; }

# Label the shapes: far wing first, then the rest of the bird, then the wordmark.
magick "$tmp/mask.png" \
    -fill 'gray(2)' -draw "color $seed floodfill" \
    -region "$bird_region" -fill 'gray(1)' -opaque white +region \
    -fill 'gray(3)' -opaque white \
    -depth 8 "gray:$tmp/labels.raw"

# Run-length encode as hex, then write the bytes.
{
    printf 'd0076b03' # 2000, 875
    od -An -v -tu1 "$tmp/labels.raw" | awk '
        function emit() { printf "%02x%02x%02x", cur, len % 256, int(len / 256) }
        {
            for (i = 1; i <= NF; i++) {
                if (len > 0 && ($i != cur || len == 65535)) { emit(); len = 0 }
                cur = $i
                len++
            }
        }
        END { if (len > 0) emit() }'
} | xxd -r -p >"$dst"

echo "$dst: 2000x875, $(wc -c <"$dst" | tr -d ' ') bytes"
