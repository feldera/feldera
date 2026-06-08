"""felderize — translate Spark SQL into Feldera SQL.

Minimal programmatic API (alternative to the CLI / shelling out):

    from felderize import translate_spark_to_feldera, Config, Status

    cfg = Config.from_env()          # reads ANTHROPIC_API_KEY, FELDERA_COMPILER, FELDERIZE_MODEL
    result = translate_spark_to_feldera(
        schema_sql,                  # str: Spark CREATE TABLE ... DDL
        query_sql,                   # str: Spark CREATE VIEW / SELECT ...
        cfg,
        validate=True,               # compile against the Feldera compiler and repair
    )

    if result.status is Status.SUCCESS:
        deploy(result.feldera_schema, result.feldera_query)
    else:
        # Status.UNSUPPORTED -> NULL-placeholder views in result.unsupported;
        # Status.ERROR       -> best-effort SQL that did not compile.
        review(result.unsupported, result.warnings)

`translate_spark_to_feldera` returns a `TranslationResult` with: `feldera_schema`,
`feldera_query`, `status`, `unsupported`, `warnings`, `explanations`, and
`to_dict()`. `validate=False` skips the compiler (faster, output unverified).
"""

from felderize.config import Config
from felderize.models import Status, TranslationResult
from felderize.translator import translate_spark_to_feldera

__all__ = [
    "translate_spark_to_feldera",
    "Config",
    "TranslationResult",
    "Status",
]
