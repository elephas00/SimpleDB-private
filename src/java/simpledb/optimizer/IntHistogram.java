package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * buckets The number of buckets to split the input value into.
     */
    private final int buckets;

    /**
     * The minimum integer value
     */
    private final int min;

    /**
     * The maximum integer value
     */
    private final int max;

    private final int[] bucketsArray;

    private int count;
    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = buckets;
        this.max = max;
        this.min = min;
        bucketsArray = new int[buckets];
        count = 0;
    }

    public static IntHistogram getInstance(int buckets, int min, int max){
        return new IntHistogram(buckets, min, max);
    }

    private int pos(int val){
        return buckets * (val - min) / (max - min + 1);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        count++;
        if(v <= min){
            bucketsArray[0]++;
            return;
        }
        if(v >= max){
            bucketsArray[buckets - 1]++;
            return;
        }
        int pos = pos(v);
        bucketsArray[pos]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if(count == 0){
            return 0d;
        }
        if(Predicate.Op.EQUALS.equals(op) || Predicate.Op.NOT_EQUALS.equals(op)){
            // calc res, the result of equals operation, then the result of not equals is 1 - res.
            double res = 0;
            if(v >= min && v <= max){
                int pos = pos(v);
                res = bucketsArray[pos] * 1.0 / count;
            }
            if(Predicate.Op.EQUALS.equals(op)){
                return res;
            }
            return 1.0 - res;
        }
        if(Predicate.Op.GREATER_THAN_OR_EQ.equals(op) || Predicate.Op.GREATER_THAN.equals(op)
            || Predicate.Op.LESS_THAN.equals(op) || Predicate.Op.LESS_THAN_OR_EQ.equals(op)){
            double res = 1.0d;
            if(v > min && v <= max){
                int pos = pos(v);
                int sum = 0;
                for(int i = pos; i < buckets; i++){
                    sum += bucketsArray[i];
                }
                res = 1.0 * sum / count;
            }else if(v > max){
                res = 0d;
            }
            if(Predicate.Op.GREATER_THAN.equals(op) || Predicate.Op.GREATER_THAN_OR_EQ.equals(op)){
                return res;
            }
            return 1.0 - res;
        }

        throw new IllegalStateException("impossible to reach here.");

    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return null;
    }
}
