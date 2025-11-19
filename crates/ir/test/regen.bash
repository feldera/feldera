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

# Extract dataflow graphs from support bundles
extract_dataflow() {
    local pipeline_name=$1
    local output_file=$2

    # Get support bundle with only dataflow graph enabled (disable other expensive collections)
    local bundle_file="${pipeline_name}-support-bundle.zip"
    fda support-bundle "$pipeline_name" \
        --no-circuit-profile \
        --no-heap-profile \
        --no-metrics \
        --no-logs \
        --no-stats \
        --no-pipeline-config \
        --no-system-config \
        --output "$bundle_file"

    # Extract dataflow_graph.json from the zip archive (take first match if multiple exist)
    local dataflow_file=$(unzip -Z1 "$bundle_file" | grep 'dataflow_graph\.json$' | head -n 1)
    unzip -p "$bundle_file" "$dataflow_file" > "$output_file"

    # Clean up the temporary bundle file
    rm "$bundle_file"
}

extract_dataflow sample_a sample_a.json
extract_dataflow sample_b sample_b.json
extract_dataflow sample_b_mod1 sample_b_mod1.json
extract_dataflow sample_b_mod2 sample_b_mod2.json
extract_dataflow sample_b_mod3 sample_b_mod3.json
extract_dataflow sample_c sample_c.json
