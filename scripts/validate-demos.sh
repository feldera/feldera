#!/bin/bash
# run the compiler on all sql examples, check against warnings
count=0
for name in crates/pipeline-manager/demos/sql/*.sql
do
    echo -n "${name#crates/pipeline-manager/demos/sql/} "
    output=$(java -jar sql-to-dbsp-compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar -i --noRust -je "$name" 2>&1)
    if [[ "$output" == *warning* ]]
    then
        echo FAIL
        count=$[$count + 1]
    else
        echo OK
    fi
done
if [ $count -gt 0 ]
then
    echo $count errors
    exit 1
fi