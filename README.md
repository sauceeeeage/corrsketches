# README

## The API
The QCR index implementation from the ICDE paper is the class `corrsketches.benchmark.index.QCRSketchIndex`. Using the default constructor of `QCRSketchIndex`, will create an in-memory index and will construct sketches using the default configurations. But you  can also customize the constructor to store the index and to use different correlation estimators as in the example bellow. To see more examples of how to use the API, you can look at the unit test class: `SketchIndexTest`.

```java
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
      new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

  ColumnPair c0 = createColumnPair(
      Arrays.asList("a", "b", "c", "d", "e"),
      new double[] {1.0, 2.0, 3.0, 4.0, 5.0});

  ColumnPair c1 = createColumnPair(
      Arrays.asList("a", "b", "c", "d"),
      new double[] {1.1, 2.5, 3.0, 4.4});

  ColumnPair c2 = createColumnPair(
      Arrays.asList("a", "b", "c"),
      new double[] {1.0, 3.1, 3.2});

  
  // The builder allows to customize the sketching method, correlation estimator, etc.
  CorrelationSketch.Builder builder = new CorrelationSketch.Builder()
      .aggregateFunction(AggregateFunction.MEAN)
      .estimator(CorrelationType.get((CorrelationType.PEARSONS)));
  boolean readonly = false;
  
  // sortBy determines the final re-ranking method after the retrieval using the QCR keys. 
  // - Sort.QCR orders hits by QCR key overlap.
  // - SortBy.CSK sorts using correlation sketches estimates. 
  SortBy sortBy = SortBy.KEY;

  // The path where to store the index. If null, an in-memory index will be created.
  String indexPath = null;
  
  // Initializes the index
  SketchIndex index = new QCRSketchIndex(indexPath, builder, sortBy, readonly );

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
```


## Building and running the code

The project uses Gradle, so you can:

Compile and run the tests:

    ./gradlew check

You can also build a runnable package using:

    ./gradlew installDist

To run it, you can run the script `benchmark` that will be generated at the `benchmark/build/install/benchmark/bin` folder, i.e.,:

    ./benchmark/build/install/benchmark/bin/benchmark

To use this method, you will need to create your code somewhere in the project and register it as a subcommand in the annotation `@Command` in the class `corrsketches.benchmark.Main`. You can look at the `corrsketches.benchmark.IndexCorrelationBenchmark` for an example.


Alternatively, you can build a single jar containing all classes:

    ./gradle shadowJar

the JAR file will be created at: `benchmark/build/libs/benchmark-0.1-SNAPSHOT-all.jar`.
You can any class with a `main()` function using the standard `java -jar` command.


