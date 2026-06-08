## Retry Context

A previous translation attempt could not handle the following Spark SQL constructs. Use the translation rules and Feldera documentation below to find equivalents for these specific items:

{unsupported_list}

Translate everything you can. If a construct still has no Feldera equivalent, insert `CAST(NULL AS <type>) /* UNSUPPORTED: <original Spark expression> */` as a placeholder and list it in `unsupported`.
