#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rpk topic delete fraud_demo_large_demographics
rpk topic delete fraud_demo_large_transactions
rpk topic delete fraud_demo_large_enriched
rpk topic delete fraud_demo_large_features

rpk topic create fraud_demo_large_demographics -c retention.ms=-1 -c retention.bytes=-1
rpk topic create fraud_demo_large_transactions -c retention.ms=-1 -c retention.bytes=-1
rpk topic create fraud_demo_large_enriched
rpk topic create fraud_demo_large_features

# Download the transaction log file.
if [ ! -f "${THIS_DIR}"/transactions.csv ]
then
    gdown 1RBEDUuvb-L15dk_UE9PPv3PgVPmkXJy6 --output "${THIS_DIR}"/transactions.csv
fi

# Push test data to topics.
printf -v pasteargs %*s 1000
while read i; do
  echo $i | rpk topic produce fraud_demo_large_demographics -f '%v'
done <  <(cat "${THIS_DIR}"/demographics.csv | paste -d "\n" ${pasteargs// /- })

printf -v pasteargs %*s 5000
while read i; do
  echo $i | rpk topic produce fraud_demo_large_transactions -f '%v'
done <  <(cat "${THIS_DIR}"/transactions.csv | paste -d "\n" ${pasteargs// /- })
