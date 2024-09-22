A compiler that translated SQL to DBSP circuits

The frontend uses Calcite to parse SQL.
It is done in three phases:

- Parse SQL to SqlNode
- Convert SqlNode to RelNode
- Optimize RelNode

The midend converts RelNode to a circuit IR