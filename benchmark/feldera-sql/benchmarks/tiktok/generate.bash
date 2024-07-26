BASEDIR="$(pwd)" # get the current directory
cd "$(dirname "${BASH_SOURCE[0]}")" # cd to the directory this script is in
TIKTOK_GEN="$(realpath ../../../../demo/project_demo12-HopsworksTikTokRecSys/tiktok-gen)"
cd $TIKTOK_GEN # cd to the tiktok generator directory
HISTORICAL="$(( ( MAX_EVENTS / 10 ) * 9 ))" # generate 90% of max events as historical events
cargo run --release -- --historical --delete-topic-if-exists -I $HISTORICAL
cargo run --release -- -I "$(( MAX_EVENTS - HISTORICAL ))"
cd $BASEDIR
