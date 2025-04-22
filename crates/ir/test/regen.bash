#!/bin/bash
fda create sample_a sample_a.sql
fda create sample_b sample_b.sql
fda create sample_b_mod1 sample_b_mod1.sql
fda create sample_b_mod2 sample_b_mod2.sql
fda create sample_b_mod3 sample_b_mod3.sql
fda create sample_c sample_c.sql

# Makes sure everything compiled up to now
fda start sample_c
fda stop sample_c
# end

fda program info sample_a | jq .dataflow > sample_a.json
fda program info sample_b | jq .dataflow > sample_b.json
fda program info sample_b_mod1 | jq .dataflow > sample_b_mod1.json
fda program info sample_b_mod2 | jq .dataflow > sample_b_mod2.json
fda program info sample_b_mod3 | jq .dataflow > sample_b_mod3.json
fda program info sample_c | jq .dataflow > sample_c.json
