#!/bin/bash

set -e

# Print usage if -h or --help is passed
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "Usage: $0 [branch-name] [dataset] [distance]"
    echo "  branch-name: Optional. The branch name to use (default: async-rework)"
    echo "  dataset: Optional. Path to dataset file (default: ../datasets/dbpedia-openai-1000k-angular.hdf5)"
    echo "  distance: Optional. Distance metric to use (default: cosine)"
    exit 0
fi

# Get parameters from arguments with defaults
BRANCH=${1:-main}
DATASET=${2:-../datasets/dbpedia-openai-1000k-angular.hdf5}
DISTANCE=${3:-cosine}

output_dir=results_$(date +%s)
mkdir -p $output_dir

# Instead of associative array, use two regular arrays
compression_names=(no lasq pq sq bq)
compression_flags=("" "--lasq enabled" "--pq enabled" "--sq enabled" "--bq")

# Function to get flags for a compression method
get_compression_flags() {
    local compression=$1
    local index=0
    for name in "${compression_names[@]}"; do
        if [ "$name" = "$compression" ]; then
            echo "${compression_flags[$index]}"
            return
        fi
        ((index++))
    done
}

# Function to run benchmark for a specific compression method
run_benchmark() {
    local compression=$1
    local flags=$(get_compression_flags "$compression")
    
    echo "Running benchmark with ${compression} compression..."
    
    if ! go run main.go ann-benchmark -v "$DATASET" -d "$DISTANCE" $flags -o results.log; then
        echo "Error: Benchmark failed for ${compression} compression"
        return 1
    fi
    
    mv results/*.json "$output_dir/${BRANCH}_${compression}_compression.json"
    jq "map(.run_id = \"${compression}_compression\")" \
        "$output_dir/${BRANCH}_${compression}_compression.json" \
        > "$output_dir/${BRANCH}_${compression}_compression_updated.json"
    rm "$output_dir/${BRANCH}_${compression}_compression.json"
}

# Cleanup results folder
rm -rf results/*

# Run benchmarks for each compression method
for compression in "${compression_names[@]}"; do
    if ! run_benchmark "$compression"; then
        echo "Benchmark failed for ${compression}. Continuing with next method..."
        continue
    fi
done

# Move all results to results directory
cp "$output_dir"/* results/