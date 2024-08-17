cd "$(dirname "$0")"

if [ ! -d "data-large" ]; then
	git clone git@github.com:dbtoaster/dbtoaster-experiments-data.git
	mv dbtoaster-experiments-data/tpch/big data-large
	mv dbtoaster-experiments-data/tpch/standard data-medium
	rm -rf dbtoaster-experiments-data
fi 

python3 generate.py