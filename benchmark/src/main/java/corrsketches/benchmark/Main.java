package corrsketches.benchmark;

import corrsketches.CorrelationSketch;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.index.Hit;
import corrsketches.benchmark.index.QCRSketchIndex;
import corrsketches.benchmark.index.SketchIndex;
import corrsketches.correlation.CorrelationType;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Command(
        subcommands = {
                ComputePairwiseJoinCorrelations.class,
                CreateColumnStore.class,
                ComputeBudget.class,
                IndexCorrelationBenchmark.class
        })
public class Main {

    public static void main(String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }
}
