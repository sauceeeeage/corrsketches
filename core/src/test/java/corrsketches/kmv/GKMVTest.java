package corrsketches.kmv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import corrsketches.kmv.GKMV.Builder;
import org.junit.jupiter.api.Test;

public class GKMVTest {

  @Test
  public void shouldEstimateNumberOfDistinctValues() {

    GKMV kmv = GKMV.builder().threshold(0.5).build();
    double maxError = 0.15;

    for (int i = 1; i <= 1000; i++) {
      kmv.update(i, i);
      if (i % 100 == 0) {
        double errorUB = 1. - (kmv.distinctValues() / i);
        double errorBE = 1. - (kmv.distinctValuesBE() / i);
        System.out.printf(
            "%-4d %-6.2f %.3f %-6.2f %.3f\n",
            i, kmv.distinctValues(), errorUB, kmv.distinctValuesBE(), errorBE);

        assertTrue(errorUB < maxError);
        assertTrue(errorBE < maxError);
      }
    }
  }

  @Test
  public void shouldEstimateIntersection() {
    // given
    double[] values = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    int[] setA1 = new int[] {1, 2, 3, 4, 5};
    int[] setA2 = new int[] {1, 2, 3, 4, 5};
    int[] setB1 = new int[] {1, 2, 3, 8, 9};
    int[] setC1 = new int[] {6, 7, 8, 9, 0};
    final Builder builder = GKMV.builder().threshold(0.5);
    // when
    GKMV kmvA1 = builder.buildFromHashedKeys(setA1, values);
    GKMV kmvA2 = builder.buildFromHashedKeys(setA2, values);
    GKMV kmvB1 = builder.buildFromHashedKeys(setB1, values);
    GKMV kmvC1 = builder.buildFromHashedKeys(setC1, values);
    // then
    double delta = 1.;
    assertEquals(5., kmvA1.intersectionSize(kmvA2), delta);
    assertEquals(3., kmvA1.intersectionSize(kmvB1), delta);
    assertEquals(0., kmvA1.intersectionSize(kmvC1), delta);
  }

  @Test
  public void shouldEstimateUnionSize() {
    // given
    double[] values = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    int[] setA1 = new int[] {1, 2, 3, 4, 5};
    int[] setA2 = new int[] {1, 2, 3, 4, 5};
    int[] setB1 = new int[] {1, 2, 3, 8, 9};
    int[] setC1 = new int[] {6, 7, 8, 9, 0};
    final Builder builder = GKMV.builder().threshold(0.8);
    // when
    GKMV kmvA1 = builder.buildFromHashedKeys(setA1, values);
    GKMV kmvA2 = builder.buildFromHashedKeys(setA2, values);
    GKMV kmvB1 = builder.buildFromHashedKeys(setB1, values);
    GKMV kmvC1 = builder.buildFromHashedKeys(setC1, values);
    // then
    double delta = 1.0;
    assertEquals(5., kmvA1.unionSize(kmvA2), delta);
    assertEquals(7., kmvA1.unionSize(kmvB1), delta);
    assertEquals(10., kmvA1.unionSize(kmvC1), delta);
  }

  @Test
  public void shouldEstimateJaccardSimilarity() {
    // given
    double[] values = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    int[] setA1 = new int[] {1, 2, 3, 4, 5};
    int[] setA2 = new int[] {1, 2, 3, 4, 5};
    int[] setB1 = new int[] {1, 2, 3, 8, 9};
    int[] setC1 = new int[] {6, 7, 8, 9, 0};
    final Builder builder = GKMV.builder().threshold(0.5);
    // when
    GKMV kmvA1 = builder.buildFromHashedKeys(setA1, values);
    GKMV kmvA2 = builder.buildFromHashedKeys(setA2, values);
    GKMV kmvB1 = builder.buildFromHashedKeys(setB1, values);
    GKMV kmvC1 = builder.buildFromHashedKeys(setC1, values);
    // then
    double delta = 0.1;
    assertEquals(1.000, kmvA1.jaccard(kmvA2), delta); // jaccard = 5/5  = 1.000
    assertEquals(0.428, kmvA1.jaccard(kmvB1), delta); // jaccard = 3/7  = 0.482
    assertEquals(0.000, kmvA1.jaccard(kmvC1), delta); // jaccard = 0/10 = 0.000
  }

  @Test
  public void shouldEstimateJaccardContainment() {
    // given
    double[] values = new double[] {1.0, 2.0, 3.0, 4.0, 5.0};
    int[] setA1 = new int[] {1, 2, 3, 4, 5};
    int[] setA2 = new int[] {1, 2, 3, 4, 5};
    int[] setB1 = new int[] {1, 2, 3, 8, 9};
    int[] setC1 = new int[] {6, 7, 8, 9, 0};
    final Builder builder = GKMV.builder().threshold(0.5);
    // when
    GKMV kmvA1 = builder.buildFromHashedKeys(setA1, values);
    GKMV kmvA2 = builder.buildFromHashedKeys(setA2, values);
    GKMV kmvB1 = builder.buildFromHashedKeys(setB1, values);
    GKMV kmvC1 = builder.buildFromHashedKeys(setC1, values);
    // then
    double delta = 0.155;
    assertEquals(1.000, kmvA1.containment(kmvA2), delta);
    assertEquals(0.600, kmvA1.containment(kmvB1), delta);
    assertEquals(0.000, kmvA1.containment(kmvC1), delta);
  }
}
