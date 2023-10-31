package corrsketches.correlation;

//TODO: should we worry about TQ only returning positive values or were we already talked about that
// and we were worried about something else, because I don't remember anything at all...(do we need to normalize it?)

import corrsketches.correlation.Correlation.Estimate;
import corrsketches.correlation.JavaMI.*;

class mi extends MutualInformation {
    public mi() {
        super();
    }
}

public class MutualInfo {
    public static Estimate estimate(double[] x, double[] y) {
        return new Estimate(coefficient(x, y), x.length);
    }

    /** implementation of the mutual information of x over y */
    //TODO: is MI the right thing to use, since it's mostly for discrete data...?
    //TODO: is the bucket size fine, or do we need to change it?
    public strictfp static double coefficient(double[] x, double[] y) {
        // x and y are the two arrays of hashed strings, I think...
        MutualInformation mi = new mi();
        return mi.calculateMutualInformation(x, y);
    }
}
