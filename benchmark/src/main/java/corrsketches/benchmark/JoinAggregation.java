package corrsketches.benchmark;

import corrsketches.aggregations.AggregateFunction;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

public class JoinAggregation {

    public static List<ColumnPair> aggregateColumnPair(
            ColumnPair cp, List<AggregateFunction> functions) {

        final Map<String, DoubleArrayList> index = createKeyIndex(cp);

        int i;
        double[][] aggregatedValues = new double[functions.size()][];
        for (i = 0; i < functions.size(); i++) {
            aggregatedValues[i] = new double[index.size()];
        }

        i = 0;
        String[] aggregatedKeys = new String[index.size()];
        for (Entry<String, DoubleArrayList> entry : index.entrySet()) {
            aggregatedKeys[i] = entry.getKey();
            for (int f = 0, functionsSize = functions.size(); f < functionsSize; f++) {
                final AggregateFunction fn = functions.get(f);
                aggregatedValues[f][i] = fn.aggregate(entry.getValue());
            }
            i++;
        }
        List<ColumnPair> results = new ArrayList<>();
        for (i = 0; i < functions.size(); i++) {
            results.add(
                    new ColumnPair(
                            cp.datasetId,
                            cp.keyName,
                            Arrays.asList(aggregatedKeys),
                            cp.columnName,
                            aggregatedValues[i]));
            // one aggregatedKeys can have multiple aggregatedValues, resulting in returning multiple ColumnPair(s) as the results
            // in our case, we only have one
        }
        return results;
    }

    public static List<NumericJoinAggregation> numericJoinAggregate(
            ColumnPair columnA, ColumnPair columnB, List<AggregateFunction> functions) {

        // TODO: Aggregate left-side of the table?
        // FIXME: aggregate using given functions

        List<String> keyValues = columnB.keyValues;

        // create index for primary key in column B
        Map<String, DoubleArrayList> indexB = createKeyIndex(columnB);
        // this index is a hashmap of <colPair.cateColVal, colPair.numColVal>
        // i.e. every cate col val(string) maps to one or more num col val(double)

        List<ColumnPair> aggregationsColumnA = aggregateColumnPair(columnA, functions);
        // ColumnPairA(s) after modified using aggregation(s)

        List<NumericJoinAggregation> results = new ArrayList<>(functions.size());

        for (int fnIdx = 0; fnIdx < functions.size(); fnIdx++) {
            final AggregateFunction fn = functions.get(fnIdx);

            ColumnPair aggregatedColumnA = aggregationsColumnA.get(fnIdx);

            // numeric values for column A
            DoubleList joinValuesA = new DoubleArrayList(aggregatedColumnA.keyValues.size());

            // numeric values for each aggregation of column B
            DoubleList joinValuesB = new DoubleArrayList();

            // compute aggregation vectors of joined values for each join key
            for (int i = 0; i < aggregatedColumnA.keyValues.size(); i++) {
                String keyA = aggregatedColumnA.keyValues.get(i);
                final double valueA = aggregatedColumnA.columnValues[i];
                final DoubleArrayList rowsB = indexB.get(keyA); // hashed join here
                if (rowsB != null && !rowsB.isEmpty()) {
                    // 1:n mapping, we aggregate joined values to a single value.
                    joinValuesA.add(valueA);
                    joinValuesB.add(fn.aggregate(rowsB));
                    // so this is basically doing the func aggregateColumnPair again, at least sort of...
                }
            }
            NumericJoinAggregation aggregation =
                    new NumericJoinAggregation(joinValuesA.toDoubleArray(), joinValuesB.toDoubleArray(), fn);
            // store the key values and the aggregated values into csv file

            // (COVID-19_Daily_Testing_-_By_Person_-_Historical.csv,Day,People Positive - Age 40-49)+(COVID-19_Daily_Testing_-_By_Test.csv,Day,Not-Positive Tests - Age 18-29).csv
            String filename = "(" + columnA.datasetId + "," + columnA.keyName + "," + columnA.columnName + ")+(" + columnB.datasetId + "," + columnB.keyName + "," + columnB.columnName + ").csv";
//            writeCSV(keyValues, aggregation, filename, columnA.keyName, columnA.columnName, columnA.columnName);

            results.add(aggregation);
            // e.g. original: 100 rows after index sketch: 10 rows and each row corresponds(aggregates) to 10 rows in original
        }

        return results;
    }

    static final String joinedFilePath = "datas/joined_data";
    private static void writeCSV(List<String> key, NumericJoinAggregation join, String filename, String k_name, String x_name, String y_name) {

//        Path path = Files.createTempFile("foo","txt");
//        Logger log = (Logger) LoggerFactory.getLogger(this.getClass());
//        try (FileInputStream fis = new FileInputStream(path.toFile());
//             FileLock lock = fis.getChannel().lock()) {
//            // unreachable code
//        } catch (NonWritableChannelException e) {
//            // handle exception
//        }
        try {
            String filenameWithoutSlash = filename.replaceAll("/", "_");
            String filenameWithoutSpace = filenameWithoutSlash.replaceAll("\\s+", "");
            Files.createDirectories(Paths.get(joinedFilePath));
            File f = new File(joinedFilePath + "/" + filenameWithoutSpace);
            if (f.createNewFile()) {
                System.out.println("File created: " + f.getName());
            } else {
                System.out.println("File already exists.");
            }
            FileWriter file = new FileWriter(f.getAbsoluteFile(), true);
            file.write(String.format("%s,%s,%s\n", k_name, x_name, y_name));

            for (int i = 0; i < join.valuesA.length; i++) {
                file.write(String.format("%s,%f,%f\n", key.get(i), join.valuesA[i], join.valuesB[i]));
                file.flush();
            }

            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, DoubleArrayList> createKeyIndex(ColumnPair column) {
        Map<String, DoubleArrayList> index = new HashMap<>();
        for (int i = 0; i < column.keyValues.size(); i++) {
            DoubleArrayList doubles = index.get(column.keyValues.get(i));
            if (doubles == null) {
                doubles = new DoubleArrayList();
                index.put(column.keyValues.get(i), doubles);
            }
            doubles.add(column.columnValues[i]);
        }
        return index;
    }

    static class NumericJoinAggregation {
        public final double[] valuesA;
        public final double[] valuesB;
        public AggregateFunction aggregate;

        public NumericJoinAggregation(double[] valuesA, double[] valuesB, AggregateFunction aggregate) {
            this.valuesA = valuesA;
            this.valuesB = valuesB;
            this.aggregate = aggregate;
        }
    }
}
