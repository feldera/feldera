#!/bin/bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

rpk topic delete green_trip_demo_large_input
rpk topic delete green_trip_demo_large_output

rpk topic create green_trip_demo_large_input -c retention.ms=-1 -c retention.bytes=-1
rpk topic create green_trip_demo_large_output

# Download the data file.
if [ ! -f "${THIS_DIR}"/green_tripdata.csv ]
then
    gdown 14cKfJjwhsVPosshmSP7MBrolsTJfz9Xh --output "${THIS_DIR}"/green_tripdata.csv
fi

# Push test data to topic.
printf -v pasteargs %*s 10000
while read i; do
  echo $i | rpk topic produce green_trip_demo_large_input -f '%v'
done <  <(cat "${THIS_DIR}"/green_tripdata.csv | paste -d "\n" ${pasteargs// /- })
