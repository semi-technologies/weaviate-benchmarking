#!/bin/bash

WEAVIATE_DIR=~/weaviate
BENCHMARKER_DIR=~/weaviate-benchmarking/benchmarker
DATASET=~/datasets/dbpedia-100k-openai-ada002-angular.hdf5
COMMIT_LIMIT=1000
HOST=b1-64gb-8c

# Clone Weaviate repository
cd $WEAVIATE_DIR

# Get the last 1000 commit hashes
commits=$(git log -n $COMMIT_LIMIT --format="%H" main)

for commit in $commits
do
    echo "Processing commit: $commit"

    cd $WEAVIATE_DIR
    
    # Checkout the commit
    git checkout $commit
    
    commit_time=$(git show -s --format=%ci $commit)
    run_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    # Start Weaviate
    ./tools/dev/run_dev_server.sh local-no-modules &
    WEAVIATE_PID=$!
    
    sleep 10
    
    # Run benchmarker
    cd $BENCHMARKER_DIR
    go run main.go ann-benchmark -v $DATASET -d cosine --queryDelaySeconds 5 --labels commit=$commit,branch=main,host=$HOST,commit_time=$commit_time,run_time=$run_time
    
    # Kill Weaviate
    kill -9 $WEAVIATE_PID
    sleep 5
    rm -rf $WEAVIATE_DIR/data
    
    # Upload results
    python import.py --directory $BENCHMARKER_DIR/results --delete
    
    echo "Finished processing commit: $commit"
done

echo "Metrics backfill completed for the last $COMMIT_LIMIT commits"