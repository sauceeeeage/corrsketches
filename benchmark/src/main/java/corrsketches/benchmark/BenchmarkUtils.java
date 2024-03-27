package corrsketches.benchmark;

import corrsketches.CorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch;
import corrsketches.CorrelationSketch.ImmutableCorrelationSketch.Paired;
import corrsketches.aggregations.AggregateFunction;
import corrsketches.benchmark.ComputePairwiseJoinCorrelations.SketchParams;
import corrsketches.benchmark.JoinAggregation.NumericJoinAggregation;
import corrsketches.benchmark.PerfResult.ComputingTime;
import corrsketches.benchmark.utils.Sets;
import corrsketches.correlation.*;
import corrsketches.correlation.BootstrapedPearson.BootstrapEstimate;
import corrsketches.correlation.Correlation.Estimate;
import corrsketches.statistics.Kurtosis;
import corrsketches.statistics.Stats;
import corrsketches.statistics.Stats.Extent;
import corrsketches.statistics.Variance;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

public class BenchmarkUtils {

    public static final int minimumIntersection = 3; // minimum sample size for correlation is 2
    static final String joinedFilePath = "./datas/joined_data";

    public static List<PerfResult> measurePerformance(
            ColumnPair x,
            ColumnPair y,
            List<SketchParams> sketchParams,
            List<AggregateFunction> functions) {

        List<PerfResult> fullJoinResults = measurePerformanceOnFullJoin(x, y, functions);

        List<PerfResult> results = new ArrayList<>();
        for (PerfResult r : fullJoinResults) {
            for (SketchParams params : sketchParams) {
                results.add(measureSketchPerformance(r.clone(), x, y, params, r.aggregate));
            }
        }

        return results;
    }

    public static PerfResult measureSketchPerformance(
            PerfResult result,
            ColumnPair x,
            ColumnPair y,
            SketchParams sketchParams,
            AggregateFunction function) {

        // create correlation sketches for the data
        long time0 = System.nanoTime();
        CorrelationSketch sketchX = createCorrelationSketch(x, sketchParams, function);
        result.build_x_time = System.nanoTime() - time0;

        time0 = System.nanoTime();
        CorrelationSketch sketchY = createCorrelationSketch(y, sketchParams, function);
        result.build_y_time = System.nanoTime() - time0;

        result.build_time = result.build_x_time + result.build_y_time;

        ImmutableCorrelationSketch iSketchX = sketchX.toImmutable();
        ImmutableCorrelationSketch iSketchY = sketchY.toImmutable();

        time0 = System.nanoTime();
        Paired paired = iSketchX.intersection(iSketchY);
        result.sketch_join_time = System.nanoTime() - time0;
        result.sketch_join_size = paired.keys.length;

        if (result.interxy_actual >= minimumIntersection && paired.keys.length >= minimumIntersection) {

            time0 = System.nanoTime();
            PearsonCorrelation.estimate(paired.x, paired.y);
            result.rp_time = System.nanoTime() - time0;

            time0 = System.nanoTime();
            QnCorrelation.estimate(paired.x, paired.y);
            result.rqn_time = System.nanoTime() - time0;

            time0 = System.nanoTime();
            SpearmanCorrelation.estimate(paired.x, paired.y);
            result.rs_time = System.nanoTime() - time0;

            time0 = System.nanoTime();
            RinCorrelation.estimate(paired.x, paired.y);
            result.rrin_time = System.nanoTime() - time0;

            time0 = System.nanoTime();
            BootstrapedPearson.estimate(paired.x, paired.y);
            result.rpm1_time = System.nanoTime() - time0;

            time0 = System.nanoTime();
            BootstrapedPearson.simpleEstimate(paired.x, paired.y);
            result.rpm1s_time = System.nanoTime() - time0;
        }

        result.parameters = sketchParams.toString();
        result.columnId =
                String.format(
                        "X(%s,%s,%s) Y(%s,%s,%s)",
                        x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

        return result;
    }

    static List<PerfResult> measurePerformanceOnFullJoin(
            ColumnPair x, ColumnPair y, List<AggregateFunction> functions) {

        PerfResult result = new PerfResult();

        HashSet<String> xKeys = new HashSet<>(x.keyValues);
        HashSet<String> yKeys = new HashSet<>(y.keyValues);
        result.cardx_actual = xKeys.size();
        result.cardy_actual = yKeys.size();
        result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);

        // No need compute any statistics when there is not intersection
        if (result.interxy_actual < minimumIntersection) {
            return Collections.emptyList();
        }

        // correlation ground-truth
        List<MetricsResult> groundTruthResults =
                computeCorrelationsAfterJoin(x, y, functions, new MetricsResult());

        List<PerfResult> perfResults = new ArrayList<>();
        for (MetricsResult r : groundTruthResults) {
            PerfResult p = result.clone();
            p.time = r.time;
            p.aggregate = r.aggregate;
            perfResults.add(p);
        }

        return perfResults;
    }

    public static List<MetricsResult> computeStatistics(
            ColumnPair x,
            ColumnPair y,
            List<SketchParams> sketchParams,
            List<AggregateFunction> functions) {

        List<MetricsResult> groundTruthResults = computeFullJoinStatistics(x, y, functions);
        // so this groundTruthResults (hash) joined on ColumnPairs' cate col
        // with the aggregate functions as modifiers for the numerical cols
        // and correlations are between the ColumnPairs' numerical cols

        List<MetricsResult> results = new ArrayList<>();
        for (MetricsResult result : groundTruthResults) {
            for (SketchParams params : sketchParams) {
                results.add(computeSketchStatistics(result.clone(), x, y, params, result.aggregate));
            }
        }

        return results;
    }

    private static List<MetricsResult> computeFullJoinStatistics(
            ColumnPair x, ColumnPair y, List<AggregateFunction> functions) {

        MetricsResult result = new MetricsResult();

        HashSet<String> xKeys = new HashSet<>(x.keyValues); // cate col values
        HashSet<String> yKeys = new HashSet<>(y.keyValues); // numerical col values
        result.cardx_actual = xKeys.size();
        result.cardy_actual = yKeys.size();
        result.interxy_actual = Sets.intersectionSize(xKeys, yKeys);

        // No need compute any statistics when there is no intersection
        if (result.interxy_actual < minimumIntersection) {
            return Collections.emptyList();
        }

        result.unionxy_actual = Sets.unionSize(xKeys, yKeys);
        result.jcx_actual = result.interxy_actual / (double) result.cardx_actual;
        result.jcy_actual = result.interxy_actual / (double) result.cardy_actual;
        result.jsxy_actual = result.interxy_actual / (double) result.unionxy_actual;

        // statistics derived from the original numeric data columns
        computeNumericColumnStatistics(x, y, result);

        // correlation ground-truth after join-aggregations
        return computeCorrelationsAfterJoin(x, y, functions, result);
    }

    private static void computeNumericColumnStatistics(
            ColumnPair x, ColumnPair y, MetricsResult result) {

        result.kurtx_g2_actual = Kurtosis.g2(x.columnValues);
        result.kurty_g2_actual = Kurtosis.g2(y.columnValues);

        final Extent extentX = Stats.extent(x.columnValues);
        result.x_min = extentX.min;
        result.x_max = extentX.max;

        final Extent extentY = Stats.extent(y.columnValues);
        result.y_min = extentY.min;
        result.y_max = extentY.max;
    }

    public static CorrelationSketch createCorrelationSketch(
            ColumnPair cp, SketchParams sketchParams, AggregateFunction function) {
        return CorrelationSketch.builder()
                .aggregateFunction(function)
                .sketchType(sketchParams.type, sketchParams.budget)
                .build(cp.keyValues, cp.columnValues);
    }

    public static MetricsResult computeSketchStatistics(
            MetricsResult result,
            ColumnPair x,
            ColumnPair y,
            SketchParams sketchParams,
            AggregateFunction function) {

        // create correlation sketches for the data
        CorrelationSketch sketchX = createCorrelationSketch(x, sketchParams, function);
        CorrelationSketch sketchY = createCorrelationSketch(y, sketchParams, function);

        ImmutableCorrelationSketch iSketchX = sketchX.toImmutable();
        ImmutableCorrelationSketch iSketchY = sketchY.toImmutable();
        Paired paired = iSketchX.intersection(iSketchY);

        // Some datasets have large column sizes, but all values can be empty strings (missing data),
        // so we need to check weather the actual cardinality and sketch sizes are large enough.
        if (result.interxy_actual >= minimumIntersection && paired.keys.length >= minimumIntersection) {

            // set operations estimates (jaccard, cardinality, etc)
            computeSetStatisticsEstimates(result, sketchX, sketchY);

            // computes statistics on joined data (e.g., correlations)
            computePairedStatistics(result, paired);
        }

        result.parameters = sketchParams.toString();
        result.columnId =
                String.format(
                        "X(%s,%s,%s) Y(%s,%s,%s)",
                        x.keyName, x.columnName, x.datasetId, y.keyName, y.columnName, y.datasetId);

        return result;
    }

    private static void computePairedStatistics(MetricsResult result, Paired paired) {

        // Sample size used to estimate correlations
        result.corr_est_sample_size = paired.keys.length;

        // correlation estimates
        Estimate estimate = PearsonCorrelation.estimate(paired.x, paired.y);
        result.corr_rp_est = estimate.coefficient;
        result.corr_rp_delta = result.corr_rp_actual - result.corr_rp_est;

        // mutual information
        Estimate miEstimate = MutualInfo.estimate(paired.x, paired.y);
        result.mi_est = miEstimate.coefficient;
        result.mi_delta = result.mi_actual - result.mi_est;

        //    if (estimate.sampleSize > 2) {
        //      // statistical significance is only defined for sample size > 2
        //      int sampleSize = estimate.sampleSize;
        //      result.corr_rp_est_pvalue2t = PearsonCorrelation.pValueTwoTailed(result.corr_rp_est,
        // sampleSize);
        //
        //      double alpha = .05;
        //      result.corr_rp_est_fisher =
        //          PearsonCorrelation.confidenceInterval(result.corr_rp_est, sampleSize, 1. - alpha);
        //      result.corr_est_significance =
        //          PearsonCorrelation.isSignificant(result.corr_rp_est, sampleSize, alpha);
        //    }

        Estimate qncorr = QnCorrelation.estimate(paired.x, paired.y);
        result.corr_rqn_est = qncorr.coefficient;
        result.corr_rqn_delta = result.corr_rqn_actual - result.corr_rqn_est;

        Estimate corrSpearman = SpearmanCorrelation.estimate(paired.x, paired.y);
        result.corr_rs_est = corrSpearman.coefficient;
        result.corr_rs_delta = result.corr_rs_actual - result.corr_rs_est;

        Estimate corrRin = RinCorrelation.estimate(paired.x, paired.y);
        result.corr_rin_est = corrRin.coefficient;
        result.corr_rin_delta = result.corr_rin_actual - result.corr_rin_est;

        BootstrapEstimate corrPm1 = BootstrapedPearson.estimate(paired.x, paired.y);
        result.corr_pm1_mean = corrPm1.corrBsMean;
        result.corr_pm1_mean_delta = result.corr_rp_actual - result.corr_pm1_mean;

        result.corr_pm1_median = corrPm1.corrBsMedian;
        result.corr_pm1_median_delta = result.corr_rp_actual - result.corr_pm1_median;

        result.corr_pm1_lb = corrPm1.lowerBound;
        result.corr_pm1_ub = corrPm1.upperBound;

        // Kurtosis of paired variables
        result.kurtx_g2 = Kurtosis.g2(paired.x);
        result.kurtx_G2 = Kurtosis.G2(paired.x);
        result.kurtx_k5 = Kurtosis.k5(paired.x);
        result.kurty_g2 = Kurtosis.G2(paired.y);
        result.kurty_G2 = Kurtosis.G2(paired.y);
        result.kurty_k5 = Kurtosis.k5(paired.y);

        final Extent extentX = Stats.extent(paired.x);
        result.x_min_sample = extentX.min;
        result.x_max_sample = extentX.max;

        final Extent extentY = Stats.extent(paired.y);
        result.y_min_sample = extentY.min;
        result.y_max_sample = extentY.max;

        double[] unitRangeX = Stats.unitize(paired.x, result.x_min, result.x_max);
        double[] unitRangeY = Stats.unitize(paired.y, result.y_min, result.y_max);
        result.x_sample_mean = Stats.mean(unitRangeX);
        result.y_sample_mean = Stats.mean(unitRangeY);
        result.x_sample_var = Variance.uvar(unitRangeX);
        result.y_sample_var = Variance.uvar(unitRangeY);
        result.nu_xy = Stats.dotn(unitRangeX, unitRangeY);
        result.nu_x = Stats.dotn(unitRangeX, unitRangeX);
        result.nu_y = Stats.dotn(unitRangeY, unitRangeY);
    }

    private static void computeSetStatisticsEstimates(
            MetricsResult result, CorrelationSketch sketchX, CorrelationSketch sketchY) {
        result.jcx_est = sketchX.containment(sketchY);
        result.jcy_est = sketchY.containment(sketchX);
        result.jsxy_est = sketchX.jaccard(sketchY);
        result.cardx_est = sketchX.cardinality();
        result.cardy_est = sketchY.cardinality();
        result.interxy_est = sketchX.intersectionSize(sketchY);
        result.unionxy_est = sketchX.unionSize(sketchY);
    }

    private static void writeCSV(List<NumericJoinAggregation> joins, String filename, String x_name, String y_name) {
        try {
            Files.createDirectories(Paths.get(joinedFilePath));
            FileWriter file = new FileWriter(joinedFilePath + "/" + filename);
            file.write(String.format("%s,%s\n", x_name, y_name));
            for (NumericJoinAggregation join : joins) {
                for (int i = 0; i < join.valuesA.length; i++) {
                    file.write(String.format("%f,%f\n", join.valuesA[i], join.valuesB[i]));
                }
            }
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<MetricsResult> computeCorrelationsAfterJoin(
            ColumnPair columnA,
            ColumnPair columnB,
            List<AggregateFunction> functions,
            MetricsResult result) {

        long time0 = System.nanoTime();
        List<NumericJoinAggregation> joins =
                JoinAggregation.numericJoinAggregate(columnA, columnB, functions);
        // basically, this joins the two column pairs on the cate column using hash joins with aggregation function(s)
        // in our case, this only have one element in the List<> for aggregation function(s)
        final long joinTime = System.nanoTime() - time0;

        List<MetricsResult> results = new ArrayList<>(functions.size());

        for (NumericJoinAggregation join : joins) {
            double[] joinedA = join.valuesA;
            double[] joinedB = join.valuesB;

            // correlation is defined only for vectors of length at least two
            if (joinedA.length < minimumIntersection) {
                continue;
            }

            MetricsResult r = result.clone();
            r.aggregate = join.aggregate;
            r.time = new ComputingTime();
            r.time.join = joinTime;

            time0 = System.nanoTime();
            r.corr_rs_actual = SpearmanCorrelation.coefficient(joinedA, joinedB);
            r.time.spearmans = System.nanoTime() - time0;

            time0 = System.nanoTime();
            r.corr_rp_actual = PearsonCorrelation.coefficient(joinedA, joinedB);
            r.time.pearsons = System.nanoTime() - time0;

            time0 = System.nanoTime();
            r.mi_actual = MutualInfo.coefficient(joinedA, joinedB);
            r.time.mi = System.nanoTime() - time0;

            time0 = System.nanoTime();
            r.corr_rin_actual = RinCorrelation.coefficient(joinedA, joinedB);
            r.time.rin = System.nanoTime() - time0;

            time0 = System.nanoTime();
            r.corr_rqn_actual = QnCorrelation.correlation(joinedA, joinedB);
            r.time.qn = System.nanoTime() - time0;

            results.add(r);

            // TODO: joins is the result of aggregation hash join on cate col
            // TODO: need to save this joins to a file for later use
            // "X(School_Survey_Effective_Leaders,Zip,Chicago_Public_Schools_-_School_Progress_Reports_SY2122.csv) Y(School_Survey_Involved_Families,Graduation_4_Year_CPS_Pct_Year_1,Chicago_Public_Schools_-_School_Progress_Reports_SY2122.csv)"
//            if (r.mi_actual > 1.1) {
////                System.out.println("mi actual greater than 1.1 for:");
////                System.out.println("X: " + columnA.columnName + " Y: " + columnB.columnName);
//                String filename = String.format("(%s,%s,%s)+(%s,%s,%s).csv", columnA.datasetId, columnA.keyName, columnA.columnName, columnB.datasetId, columnB.keyName, columnB.columnName);
////                System.out.println("filename: " + filename);
////                try {
////                    FileWriter file = new FileWriter(joinedFilePath + "/" + "mi_greater_than_1.1.txt", true);
////                    file.write(filename);
////                    file.write("\n");
////                    file.close();
////                } catch (IOException e) {
////                    throw new RuntimeException(e);
////                }
////                writeCSV(joins, filename, columnA.columnName, columnB.columnName);
//            }
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream("joined_datasets.csv", true), "utf-8"))) {
                if (Files.size(Paths.get("joined_datasets.csv")) == 0) {
                    writer.write("table1,cate1,num1,table2,cate2,num2\n");
                }
                writer.write(columnA.datasetId + "," + columnA.keyName + "," + columnA.columnName + "," + columnB.datasetId + "," + columnB.keyName + "," + columnB.columnName + "\n");
                writer.flush();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return results;
    }
}
