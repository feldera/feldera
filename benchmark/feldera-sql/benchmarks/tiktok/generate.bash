BASEDIR="$(pwd)" # get the current directory
cd "$(dirname "${BASH_SOURCE[0]}")" # cd to the directory this script is in
TIKTOK_GEN="$(realpath ../../../../demo/project_demo12-HopsworksTikTokRecSys/tiktok-gen)"
cd $TIKTOK_GEN # cd to the tiktok generator directory
cargo run --release -- --historical --delete-topic-if-exists -I ${MAX_EVENTS:-50000000}
cd $BASEDIR
