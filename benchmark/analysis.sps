/* This is SPSS syntax for generating a summary table.  It works with
/* GNU PSPP's current tip-of-master (the CTABLES command isn't supported
/* in the latest release version).

DATA LIST LIST(',') NOTABLE FILE='|grep -vh when *-100M.csv'
   /when (YMDHMS17)
    runner (A15)
    mode (A6)
    language (A8)
    query (a3)
    cores (F4.1)
    events (F9)
    elapsed (F5.3).
VARIABLE LEVEL cores events elapsed (SCALE) query (NOMINAL).

SELECT IF mode='stream'.

VALUE LABELS language
  'sql' 'SQL'
  'zetasql' 'ZetaSQL'.

COMPUTE querynum = NUMBER(SUBSTR(query, 2), F8).
VARIABLE LABEL querynum 'query'.
FORMATS querynum(F2.0).
VARIABLE LEVEL querynum (NOMINAL).

RECODE runner
  ('dbsp'=0)
  ('flink'=1)
  ('beam.direct'=2)
  ('beam.flink'=3)
  ('beam.spark'=4)
  ('beam.dataflow'=5)
  (ELSE=SYSMIS)
  INTO nrunner.
VARIABLE LABEL nrunner 'runner'.
VALUE LABELS nrunner
  0 'DBSP'
  1 'Flink'
  2 'Beam (direct)'
  3 'Flink on Beam'
  4 'Spark on Beam'
  5 'Dataflow on Beam'.
VARIABLE LEVEL nrunner (NOMINAL).

COMPUTE eps=events/elapsed.
VARIABLE LABEL eps 'events/s'.
FORMATS eps(COMMA10).
CTABLES
    /TABLE=querynum BY nrunner > language > eps
    /TITLES
     TITLE='16-core Nexmark Streaming Performance'
     CAPTION='Beam Spark performance omitted because Nexmark hangs in streaming mode.'.
