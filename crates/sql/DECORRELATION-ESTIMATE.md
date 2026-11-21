# Decorrelation scope after Spike 6

The original compiler port covered **9 of 155 first-inner-stage Correlate
occurrences through C7 (5.81%)**. Six more occurrences rewrite and reconstruct
without credited corpus C7. Crediting all six would reach 9.68%. Spike 6 falls
below the 95% criterion, and risk 2 is High. This selected-stage measurement
covers 60 cases and 75 invocations. The complete four-stage classic census
covers 8,571 cases, including 157 with correlation or subquery observations.
[Adjudication](SPIKE6-ADJUDICATION.md) · [Occurrence evidence](evidence/correlation-coverage.json)

The existing prototype handles Project, Filter, Set and scalar COUNT, MIN
and MAX. Its runtime fixtures exercise distinct domains, empty groups, COUNT
correction, NULL-safe domain matching, HAVING, LEFT padding, and duplicate
retractions.

Of the 59 invocations the original experiment refused, 30 involved
grouped aggregates, 15 involved unsupported aggregate calls or metadata,
10 involved joins, windows, sorts, UNNEST or nested structures, three
involved hints, and one involved foreign correlation. Together these represent 138 occurrences. Another two
occurrences are in one RexSubQuery transport failure.

Prioritize ordinary grouped aggregates and richer aggregate calls. Test their
S1 reconstruction and signed C7 results against the original corpus before
crediting occurrences. Handle joined and nested domains, windows, sorts,
UNNEST, hint preservation and transport as separate semantic changes. Keep
the original failed result and the four-stage census in the evidence record.

The grouped-aggregate follow-up adds the correlation domain to an ordinary
GROUP BY and preserves empty grouped results. The exact original population now
has 108 C7-equal occurrences, 14 reconstructed without C7, 31 refused and two
transport failures. Run-time evidence covers 121 transactions in 26 original
classic cases. [Grouped aggregate evidence](evidence/correlation-grouped.json)

Adding ordinary SUM, AVG and SINGLE_VALUE raises verified coverage to
119 of 155 occurrences and preserves 136 transactions in 37 original
classic cases. [Aggregate call evidence](evidence/correlation-aggregate-calls.json)

Ordered partitioned FETCH raises verified coverage to 129 of 155 occurrences.
The 47 original classic cases pass 146 saved transactions.
[Partitioned FETCH evidence](evidence/correlation-partitioned-fetch.json)

The final selected-stage follow-up verifies 149 of 155 occurrences.
The saved-oracle tests verify 130 and newly seeded, paired classic runs
of ten compile-only programs verify 19. The six remaining occurrences
include two unsupported nested correlations and the original subquery
transport failure. [Weighted C7 evidence](evidence/correlation-spike6-weighted.json)
