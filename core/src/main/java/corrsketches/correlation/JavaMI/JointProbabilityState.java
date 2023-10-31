/*******************************************************************************
 ** JointProbabilityState.java
 ** Part of the Java Mutual Information toolbox
 **
 ** Author: Adam Pocock
 ** Created: 20/1/2012
 **
 **  Copyright 2012-2016 Adam Pocock, The University Of Manchester
 **  www.cs.manchester.ac.uk
 **
 **  This file is part of JavaMI.
 **
 **  JavaMI is free software: you can redistribute it and/or modify
 **  it under the terms of the GNU Lesser General Public License as published by
 **  the Free Software Foundation, either version 3 of the License, or
 **  (at your option) any later version.
 **
 **  JavaMI is distributed in the hope that it will be useful,
 **  but WITHOUT ANY WARRANTY; without even the implied warranty of
 **  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 **  GNU Lesser General Public License for more details.
 **
 **  You should have received a copy of the GNU Lesser General Public License
 **  along with JavaMI.  If not, see <http://www.gnu.org/licenses/>.
 **
 *******************************************************************************/

package corrsketches.correlation.JavaMI;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Arrays;
import java.util.Collections;

import com.google.common.primitives.Doubles;

/**
 * Calculates the probabilities of each state in a joint random variable.
 * Provides the base for all functions of two variables.
 *
 * @author apocock
 */
public class JointProbabilityState {
    public final HashMap<Pair<Integer, Integer>, Double> jointProbMap;
    public final HashMap<Integer, Double> firstProbMap;
    public final HashMap<Integer, Double> secondProbMap;

    //  public final double jointMaxVal;
    public final double firstMaxVal;
    public final double firstMinVal;
    public final double secondMaxVal;
    public final double secondMinVal;

    /**
     * Constructor for the JointProbabilityState class. Takes two data vectors and calculates
     * the joint and marginal probabilities, before storing them in HashMaps.
     *
     * @param firstVector  Input vector. It is discretised to the floor of each value.
     * @param secondVector Input vector. It is discretised to the floor of each value.(0..maxVal-1)
     */
    public JointProbabilityState(double[] firstVector, double[] secondVector, int numOfBuckets) {
        jointProbMap = new HashMap<Pair<Integer, Integer>, Double>();
        firstProbMap = new HashMap<Integer, Double>();
        secondProbMap = new HashMap<Integer, Double>();

        int firstVal, secondVal;
        Pair<Integer, Integer> jointVal;
        // tmpKey is used to reduce the amount of autoboxing, and is probably premature optimisation
        Integer tmpKey, count;

        int vectorLength = Math.max(firstVector.length, secondVector.length);
//        double doubleLength = Math.max(firstVector.length, secondVector.length);

        //FIXME： don't think we need to normalize those vectors, since they came in as hashed values
        //round input to integers
//    int[] firstNormalisedVector = new int[vectorLength];
//    int[] secondNormalisedVector = new int[vectorLength];
//    firstMaxVal = ProbabilityState.normaliseArray(firstVector,firstNormalisedVector);
//    secondMaxVal = ProbabilityState.normaliseArray(secondVector,secondNormalisedVector);
        firstMaxVal = Doubles.max(firstVector);
        firstMinVal = Doubles.min(firstVector);
        secondMaxVal = Doubles.max(secondVector);
        secondMinVal = Doubles.min(secondVector);
        //TODO: use equal width bucketization for now. Use the max value to determine the number of buckets
        //TODO: run end to end tests to see if this is a good idea(this Java code gives out MI coefficients and the columns that associate with them, double check them with the python code)
        //TODO: check LGPL license for this code

        // set up the bucket boundaries
        double[] firstVecBucketBoundary = new double[numOfBuckets]; // min val of first vector + numOfBuckets-1 + max val of first vector(all inclusive)
        double[] secondVecBucketBoundary = new double[numOfBuckets]; // min val of second vector + numOfBuckets-1 + max val of second vector(all inclusive)
        //FIXME: hopefully no precision issue here
        double firstBucketWidth = firstMaxVal / numOfBuckets;
        double secondBucketWidth = secondMaxVal / numOfBuckets;
        firstVecBucketBoundary[0] = firstMinVal;
        secondVecBucketBoundary[0] = secondMinVal;
        for (int i = 1; i < numOfBuckets; i++) {
            firstVecBucketBoundary[i] = i * firstBucketWidth;
            secondVecBucketBoundary[i] = i * secondBucketWidth;
        }
//        firstVecBucketBoundary[numOfBuckets] = firstMaxVal;
//        secondVecBucketBoundary[numOfBuckets] = secondMaxVal;
        // end of setting up the bucket boundaries

        // bucketize the input vectors
        int[] bucketizedFirstVector = new int[vectorLength];
        int[] bucketizedSecondVector = new int[vectorLength];
        for (int i = 0; i < vectorLength; i++) {
            // initialize the bucketized vectors with -1s, so the short vectors will be padded with -1s
            bucketizedFirstVector[i] = -1;
            bucketizedSecondVector[i] = -1;
        }
        for (int i = 0; i < firstVector.length; i++) {
            bucketizedFirstVector[i] = bucketize(firstVector[i], firstVecBucketBoundary);
        }
        for (int i = 0; i < secondVector.length; i++) {
            bucketizedSecondVector[i] = bucketize(secondVector[i], secondVecBucketBoundary);
        }
        // end of bucketizing the input vectors

        HashMap<Pair<Integer, Integer>, Integer> jointCountMap = new HashMap<Pair<Integer, Integer>, Integer>();
        HashMap<Integer, Integer> firstCountMap = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> secondCountMap = new HashMap<Integer, Integer>();

        for (int i = 0; i < vectorLength; i++) {
            firstVal = bucketizedFirstVector[i];
            secondVal = bucketizedSecondVector[i];
            if (firstVal == -1 || secondVal == -1) {
                continue;
            }//FIXME: skip the padded -1s in the short vectors for now, idk what we want to do with them
            jointVal = new Pair<Integer, Integer>(firstVal, secondVal);

            count = jointCountMap.get(jointVal);
            if (count == null) {
                jointCountMap.put(jointVal, 1);
            } else {
                jointCountMap.put(jointVal, count + 1);
            }

//        tmpKey = firstVal;
            count = firstCountMap.get(firstVal);
            if (count == null) {
                firstCountMap.put(firstVal, 1);
            } else {
                firstCountMap.put(firstVal, count + 1);
            }

//        tmpKey = secondVal;
            count = secondCountMap.remove(secondVal);
            if (count == null) {
                secondCountMap.put(secondVal, 1);
            } else {
                secondCountMap.put(secondVal, count + 1);
            }
        }

        //FIXME: not sure if we should devide by vectorLength or firstVector.length or secondVector.length
        for (Entry<Pair<Integer, Integer>, Integer> e : jointCountMap.entrySet()) {
            jointProbMap.put(e.getKey(), e.getValue() / (double) vectorLength);
        }

        for (Entry<Integer, Integer> e : firstCountMap.entrySet()) {
            firstProbMap.put(e.getKey(), e.getValue() / (double) firstVector.length);
        }

        for (Entry<Integer, Integer> e : secondCountMap.entrySet()) {
            secondProbMap.put(e.getKey(), e.getValue() / (double) secondVector.length);
        }
    }//constructor(double[],double[])

    private int bucketize(double v, double[] vecBucketBoundary) {
        /***
         * the buckets are in [vecBucketBoundary[i], vecBucketBoundary[i+1])...[vecBucketBoundary[len-2], vecBucketBoundary[len-1]] fashion
         * e.g. vec = [0.0-4.0]
         * vecBucketBoundary = [0,1,2,3]
         * bucketize(0.0) = 0
         * bucketize(1.0) = 1
         * bucketize(3.0) = 3
         * bucketize(4.0) = 3
         */
        //FIXME: could use binary search here, but since the number of buckets is small, linear search is fine
        int bucket = 0;
        for (int i = 0; i < vecBucketBoundary.length; i++) {
            if (v >= vecBucketBoundary[i]) {
                bucket = i;
            }
        }
        return bucket;
    }
}//class JointProbabilityState
