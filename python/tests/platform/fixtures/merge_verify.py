"""Read a merge-mode Delta table with Delta Spark, through table maintenance.

Prints one JSON object per phase (`initial`, `after_optimize`, `after_vacuum`),
all from one process because starting the JVM costs far more than the reads.
Invoked by `tests.utils.run_delta_spark`; needs `delta-spark`, not bare pyspark,
since deletion-vector support lives in the Delta Lake Spark JARs.
"""

import json
import sys


def _spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("feldera-merge-verify")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Zero-retention vacuum is the most aggressive one a user can run, which
        # is what makes it the right question to ask about live vector files.
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> int:
    path = sys.argv[1]
    spark = _spark()
    table = f"delta.`{path}`"

    def report(phase: str) -> None:
        rows = [
            row.asDict(recursive=True)
            for row in spark.sql(f"SELECT * FROM {table}").collect()
        ]
        print(json.dumps({"phase": phase, "rows": rows}, sort_keys=True, default=str))

    report("initial")
    spark.sql(f"OPTIMIZE {table}")
    report("after_optimize")
    spark.sql(f"VACUUM {table} RETAIN 0 HOURS")
    report("after_vacuum")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
