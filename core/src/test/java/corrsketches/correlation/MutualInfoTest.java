package corrsketches.correlation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MutualInfoTest {
    @Test
    public void shouldComputeMutualInfo() {
        double[] x = {1, 2, 3};
        double[] x1 = {1, 2, 3, 4};
        double[] y = {1, 2, 3};
        double[] y1 = {1, 2, 3, 4};
        assertEquals(1.584962500721156, MutualInfo.coefficient(x, y));
        assertEquals(1.1887218755408673, MutualInfo.coefficient(x, y1));
        assertEquals(1.1887218755408673, MutualInfo.coefficient(x1, y));
        assertEquals(2.0, MutualInfo.coefficient(x1, y1));
    }
}
