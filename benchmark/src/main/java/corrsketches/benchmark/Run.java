package corrsketches.benchmark;

import corrsketches.CorrelationSketch;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.index.Hit;
import corrsketches.benchmark.index.QCRSketchIndex;
import corrsketches.benchmark.index.SketchIndex;
import corrsketches.benchmark.utils.CliTool;
import corrsketches.correlation.CorrelationType;
import picocli.CommandLine;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Run {

    static ColumnPair createColumnPair(List<String> keyValues, double[] columnValues) {
        ColumnPair cp = new ColumnPair();
        cp.columnValues = columnValues;
        cp.keyValues = keyValues;
        return cp;
    }

    public static void main(String[] args) throws Exception {
        if (Objects.equals(System.getenv("RUN_SHAOTONG_TEST"), "true")) {
            System.out.println("Running Shaotong's test");
            // Creates data samples. createColumnPair() is a simple function that
            // instantiates the ColumnPair objects. Its implementation is availabe
            // in the class SketchIndexTest, but you just need to set the data to
            // ColumnPair objects as follows:
            //
            //   ColumnPair cp = new ColumnPair();
            //   cp.columnValues = columnValues;
            //   cp.keyValues = keyValues;
            //
            ColumnPair q = createColumnPair(
                    Arrays.asList("a", "b", "c", "d", "e"),
                    new double[]{1.0, 2.0, 3.0, 4.0, 5.0});

            ColumnPair c0 = createColumnPair(
                    Arrays.asList("a", "b", "c", "d", "e"),
                    new double[]{1.0, 2.0, 3.0, 4.0, 5.0});

            ColumnPair c1 = createColumnPair(
                    Arrays.asList("a", "b", "c", "d"),
                    new double[]{1.1, 2.5, 3.0, 4.4});

            ColumnPair c2 = createColumnPair(
                    Arrays.asList("a", "b", "c"),
                    new double[]{1.0, 3.1, 3.2});


            // The builder allows to customize the sketching method, correlation estimator, etc.
            CorrelationSketch.Builder builder = new CorrelationSketch.Builder()
                    .aggregateFunction(AggregateFunction.MEAN)
                    .estimator(CorrelationType.get((CorrelationType.MI)));
            boolean readonly = false;

            // sortBy determines the final re-ranking method after the retrieval using the QCR keys.
            // - Sort.QCR orders hits by QCR key overlap.
            // - SortBy.CSK sorts using correlation sketches estimates.
//          IndexCorrelationBenchmark.SortBy sortBy = IndexCorrelationBenchmark.SortBy.CSK;
            IndexCorrelationBenchmark.SortBy sortBy = IndexCorrelationBenchmark.SortBy.KEY;

            // The path where to store the index. If null, an in-memory index will be created.
            String indexPath = null;

            // Initializes the index
            SketchIndex index = new QCRSketchIndex(indexPath, builder, sortBy, readonly);

            // Creates sketches and adds them to the index
            index.index("c0", c0);
            index.index("c1", c1);
            index.index("c2", c2);
            // sketches will appear on searches only after a refresh.
            index.refresh();
            // retrieve top-5 items for the query q
            List<Hit> hits = index.search(q, 5);

            System.out.println("Total hits: " + hits.size());
            for (int i = 0; i < hits.size(); i++) {
                Hit hit = hits.get(i);
                System.out.printf("\n[%d] ", i + 1);
                // the id used to index the sketch ("c0", "c1", etc)
                System.out.println("id: " + hit.id);
                // the keys overlap computed by the index processing
                System.out.println("    score: " + hit.score);
                // estimated using the sketches
                System.out.println("    correlation: " + hit.correlation());
            }
        }
    }
}

/***
 * CorrelationType.MI result:
 * SortBy.CSK:
 * Total hits: 3
 *
 * [1] id: c0
 *     score: 5.0
 *     correlation: 2.3219280948873626
 *
 * [2] id: c1
 *     score: 3.0
 *     correlation: 2.0
 *
 * [3] id: c2
 *     score: 1.0
 *     correlation: 0.9182958340544894
 *
 *
 * SortBy.KEY:
 * Total hits: 3
 *
 * [1] id: c0
 *     score: 5.0
 *     correlation: 2.3219280948873626
 *
 * [2] id: c1
 *     score: 3.0
 *     correlation: 2.0
 *
 * [3] id: c2
 *     score: 1.0
 *     correlation: 0.9182958340544894
 *
 * -----------------------------------------------------------------------------------------------
 *
 * CorrelationType.PEARSON result:
 * SortBy.CSK:
 * Total hits: 3
 *
 * [1] id: c0
 *     score: 5.0
 *     correlation: 1.0
 *
 * [2] id: c1
 *     score: 3.0
 *     correlation: 0.985350505855448
 *
 * [3] id: c2
 *     score: 1.0
 *     correlation: 0.8854475018981705
 *
 *
 * SortBy.KEY:
 * Total hits: 3
 *
 * [1] id: c0
 *     score: 5.0
 *     correlation: 1.0
 *
 * [2] id: c1
 *     score: 3.0
 *     correlation: 0.985350505855448
 *
 * [3] id: c2
 *     score: 1.0
 *     correlation: 0.8854475018981705
 */
