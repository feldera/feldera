BASEDIR="$(pwd)"
cd "$(dirname "${BASH_SOURCE[0]}")"
TIKTOK_GEN="$(realpath ../../../../demo/project_demo12-HopsworksTikTokRecSys/tiktok-gen)"
cd $TIKTOK_GEN
cargo run --release -- --historical --delete-topic-if-exists
cargo run --release -- -I 1000000
cd $BASEDIR
