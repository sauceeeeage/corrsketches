INPUT_PATH=${INPUT_PATH:-"./datas/tables"}
STORE_PATH=${STORE_PATH:-"./datas/output_store"}
RESULTS_PATH=${RESULTS_PATH:-"./datas/results"}
SKETCH_TYPE=${SKETCH_TYPE:-"KMV"}
BUDGET=${BUDGET:-256}
DBTYPE=${DBTYPE:-"ROCKSDB"}

JAR=benchmark/build/libs/benchmark-0.1-SNAPSHOT-all.jar
BENCHMARK_EXE="java -cp $JAR corrsketches.benchmark.ComputePairwiseJoinCorrelations"
CREATE_STORE_EXE="java -cp $JAR corrsketches.benchmark.CreateColumnStore"

CREATE_STORE_CMD="$CREATE_STORE_EXE --input-path $INPUT_PATH --output-path $STORE_PATH --db-backend $DBTYPE"
BENCHMARK_CMD="$BENCHMARK_EXE --input-path $STORE_PATH --output-path $RESULTS_PATH --sketch-params=$SKETCH_TYPE:$BUDGET"

echo "Compiling application..."
gradle shadowJar

echo "Create Column Key-Value Store..."
echo "Command: $CREATE_STORE_CMD"
$CREATE_STORE_CMD

echo "Running benchmark..."
echo "Command: $BENCHMARK_CMD"
$BENCHMARK_CMD
