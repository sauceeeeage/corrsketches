package corrsketches.benchmark;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import corrsketches.SketchType;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.CreateColumnStore.ColumnStoreMetadata;
import corrsketches.benchmark.utils.CliTool;
import hashtabledb.BytesBytesHashtable;
import hashtabledb.Kryos;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
        name = ComputePairwiseJoinCorrelations.JOB_NAME,
        description = "Compute correlation after joins of each pair of categorical-numeric column")
public class ComputePairwiseJoinCorrelations extends CliTool implements Serializable {

    public static final String JOB_NAME = "ComputePairwiseJoinCorrelations";

    public static final Kryos<ColumnPair> KRYO = new Kryos<>(ColumnPair.class);

    @Option(
            names = "--input-path",
            required = true,
            description = "Folder containing key-value column store")
    String inputPath;

    @Option(names = "--output-path", required = true, description = "Output path for results file")
    String outputPath;

    @Option(
            names = "--sketch-params",
            required = true,
            description =
                    "A comma-separated list of sketch parameters in the format SKETCH_TYPE:BUDGET_VALUE,SKETCH_TYPE:BUDGET_VALUE,...")
    String sketchParams = null;

    @Option(
            names = "--intra-dataset-combinations",
            description = "Whether to consider only intra-dataset column combinations")
    Boolean intraDatasetCombinations = false;

    @Option(names = "--performance", description = "Run performance experiments")
    Boolean performance = false;

    @Option(
            names = "--max-combinations",
            description = "The maximum number of columns to consider for creating combinations.")
    private int maxSamples = 5000;

    @Option(
            names = "--cpu-cores",
            description = "Number of CPU core to use. Default is to use all cores available.")
    int cpuCores = -1;

    @Option(names = "--aggregations", description = "Change the default(mean) aggregate functions")
    String aggregateFunctions = "MEAN";

    public static void main(String[] args) {
        CliTool.run(args, new ComputePairwiseJoinCorrelations());
    }

    @Override
    public void execute() throws Exception {
        List<SketchParams> sketchParamsList = SketchParams.parse(this.sketchParams);
        System.out.println("> SketchParams: " + this.sketchParams);

        List<AggregateFunction> aggregations = parseAggregations(this.aggregateFunctions);
        System.out.println("> Using aggregate functions: " + aggregations);

        ColumnStoreMetadata storeMetadata = CreateColumnStore.readMetadata(inputPath);
        BytesBytesHashtable columnStore = new BytesBytesHashtable(storeMetadata.dbType, inputPath);

        Set<Set<String>> columnSets = storeMetadata.columnSets;
        // this column sets contains set of set of cp.id()s, which is in the hierarchy of all csv files-> specific csv file-> a cp.id()
        System.out.println(
                "> Found  " + columnSets.size() + " column pair sets in DB stored at " + inputPath);

        System.out.println("\n> Computing column statistics for all column combinations...");
        Set<ColumnCombination> combinations =
                ColumnCombination.createColumnCombinations(
                        columnSets, intraDatasetCombinations, maxSamples);
        // this is a combinations of cp.id() pairs mixed using combination method(C(x,y))

        String baseInputPath = Paths.get(inputPath).getFileName().toString();
        String filename =
                String.format("%s_sketch-params=%s.csv", baseInputPath, sketchParams.toLowerCase());
        if (performance) {
            filename = filename.replace("_sketch-params=", "_perf_sketch-params=");
        }

        Files.createDirectories(Paths.get(outputPath));
        FileWriter resultsFile = new FileWriter(Paths.get(outputPath, filename).toString());

        if (performance) {
            resultsFile.write(PerfResult.csvHeader() + "\n");
        } else {
            resultsFile.write(MetricsResult.csvHeader() + "\n");
        }

        System.out.println("Number of combinations: " + combinations.size());
        final AtomicInteger processed = new AtomicInteger(0);
        final int total = combinations.size();

        Cache<String, ColumnPair> cache = CacheBuilder.newBuilder().softValues().build();
        // the ColumnPair in this cache struct is actually the full column pair
        // that include the csv name, both a cate col and a numerical col's name and values

        int cores = cpuCores > 0 ? cpuCores : Runtime.getRuntime().availableProcessors();
        ForkJoinPool forkJoinPool = new ForkJoinPool(cores);
        final Runnable task;
        if (performance) {
            task =
                    () ->
                            combinations.stream()
                                    .parallel()
                                    .map(
                                            computePerformanceStatistics(
                                                    cache, columnStore, processed, total, sketchParamsList, aggregations))
                                    .forEach(writeCSV(resultsFile));
        } else {
            task =
                    () ->
                            combinations.stream()
                                    .parallel()
                                    .map(
                                            computeStatistics(
                                                    cache, columnStore, processed, total, sketchParamsList, aggregations))
                                    .forEach(writeCSV(resultsFile));
            // this is weird, b/c this .map func for computeStatistics basically compute stat values for all ColumnPairs
            // shouldn't there be a better way to do so, cutting some cost down???
        }
        forkJoinPool.submit(task).get();

        resultsFile.close();
        columnStore.close();
        System.out.println(getClass().getSimpleName() + " finished successfully.");
    }

    private List<AggregateFunction> parseAggregations(String functions) {
        if (functions == null || functions.isEmpty()) {
            System.out.println("No aggregate functions configured. Using default: FIRST");
            return Collections.singletonList(AggregateFunction.FIRST);
        }
        if (functions.equals("all")) {
            return AggregateFunction.all();
        }
        final List<AggregateFunction> aggregateFunctions = new ArrayList<>();
        for (String functionName : functions.split(",")) {
            try {
                aggregateFunctions.add(AggregateFunction.valueOf(functionName));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "Unrecognized aggregate functions name: " + functionName, e);
            }
        }
        return aggregateFunctions;
    }

    private Function<ColumnCombination, String> computeStatistics(  
            Cache<String, ColumnPair> cache,
            BytesBytesHashtable hashtable,
            AtomicInteger processed,
            double total,
            List<SketchParams> params,
            List<AggregateFunction> functions) {
        return (ColumnCombination columnPair) -> {
            ColumnPair x = getColumnPair(cache, hashtable, columnPair.x); // columnPair.x and .y are just cp.id()
            ColumnPair y = getColumnPair(cache, hashtable, columnPair.y);

            List<MetricsResult> results = BenchmarkUtils.computeStatistics(x, y, params, functions);

            int current = processed.incrementAndGet();
            if (current % 1000 == 0) {
                double percent = 100 * current / total;
                synchronized (System.out) {
                    System.out.println("\r");
                    System.out.printf("Progress: %.3f%%\n", percent);
                }
            }

            if (results == null || results.isEmpty()) {
                return "";
            } else {
                StringBuilder builder = new StringBuilder();
                for (MetricsResult result : results) {
                    // we don't need to report column pairs that have no intersection at all
                    if (Double.isFinite(result.interxy_actual) && result.interxy_actual >= 2) {
                        builder.append(result.csvLine());
                        builder.append('\n');
                    }
                }
                return builder.toString();
            }
        };
    }

    private Function<ColumnCombination, String> computePerformanceStatistics(
            Cache<String, ColumnPair> cache,
            BytesBytesHashtable hashtable,
            AtomicInteger processed,
            double total,
            List<SketchParams> params,
            List<AggregateFunction> aggregations) {
        return (ColumnCombination columnPair) -> {
            ColumnPair x = getColumnPair(cache, hashtable, columnPair.x);
            ColumnPair y = getColumnPair(cache, hashtable, columnPair.y);

            List<PerfResult> results = BenchmarkUtils.measurePerformance(x, y, params, aggregations);

            int current = processed.incrementAndGet();
            if (current % 1000 == 0) {
                double percent = 100 * current / total;
                synchronized (System.out) {
                    System.out.printf("Progress: %.3f%%\n", percent);
                }
            }

            if (results == null || results.isEmpty()) {
                return "";
            } else {
                StringBuilder builder = new StringBuilder();
                for (PerfResult result : results) {
                    // we don't need to report column pairs that have no intersection at all
                    if (Double.isFinite(result.interxy_actual) && result.interxy_actual >= 2) {
                        builder.append(result.csvLine());
                        builder.append('\n');
                    }
                }
                return builder.toString();
            }
        };
    }

    private ColumnPair getColumnPair(
            Cache<String, ColumnPair> cache, BytesBytesHashtable hashtable, String key) {
        ColumnPair cp = cache.getIfPresent(key);
        if (cp == null) {
            byte[] keyBytes = key.getBytes();
            cp = KRYO.unserializeObject(hashtable.get(keyBytes));
            cache.put(key, cp);
        }
        return cp;
    }

    private Consumer<String> writeCSV(FileWriter file) {
        return (String line) -> {
            if (!line.isEmpty()) {
                synchronized (file) {
                    try {
                        file.write(line);
                        file.flush();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to write line to file: " + line);
                    }
                }
            }
        };
    }

    public static class SketchParams {

        public final SketchType type;
        public final double budget;

        public SketchParams(SketchType type, double budget) {
            this.type = type;
            this.budget = budget;
        }

        public static List<SketchParams> parse(String params) {
            String[] values = params.split(",");
            List<SketchParams> result = new ArrayList<>();
            for (String value : values) {
                result.add(parseValue(value.trim()));
            }
            if (result.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("[%s] does not have any valid sketch parameters", params));
            }
            return result;
        }

        public static SketchParams parseValue(String params) {
            String[] values = params.split(":");
            if (values.length == 2) {
                return new SketchParams(
                        SketchType.valueOf(values[0].trim()), Double.parseDouble(values[1].trim()));
            } else {
                throw new IllegalArgumentException(String.format("[%s] is not a valid parameter", params));
            }
        }

        @Override
        public String toString() {
            return type.toString() + ":" + budget;
        }
    }
}
