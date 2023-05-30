package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.List;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private List<Integer> stats;
    private int buckets;
    //元素总数
    private int elemCount;
    private double step;
    private int min;
    private int max;
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
        stats = new ArrayList<>(buckets);
        for(int i = 0; i < buckets; i++) {
            stats.add(0);
        }
        this.min = min;
        this.max = max;
        step = (double) (max - min) / buckets;
        elemCount = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(v < min || v > max) {
            return;
        }
        int index =(int)((v - min)/ step);
        if(index == stats.size()) {
            index--;
        }
        int count = stats.get(index) + 1;
        stats.set(index, count);
        elemCount++;
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
        int index =(int)((v - min)/ step);
        if(index == stats.size()) {
            index--;
        }

        if(Predicate.Op.EQUALS.equals(op)) {
            if(index < 0 || index >= stats.size()) {
                return 0.0;
            }
            return (double)stats.get(index) / step / elemCount;
        } else if(Predicate.Op.NOT_EQUALS.equals(op)) {
            if(index < 0 || index >= stats.size()) {
                return 1.0;
            }
            return 1 - stats.get(index) / step / elemCount;
        }
        else if(Predicate.Op.GREATER_THAN.equals(op) || Predicate.Op.GREATER_THAN_OR_EQ.equals(op)) {
            if(v <= min) {
                return 1.0;
            } else if(v >= max) {
                return 0.0;
            }
            double count = (min + step * (index + 1) - v) * (double) stats.get(index) / step;
            for(int i = index + 1; i < stats.size(); i++) {
                count += stats.get(i);
            }
            if(Predicate.Op.GREATER_THAN_OR_EQ.equals(op)) {
                return count / elemCount + (double)stats.get(index) / step / elemCount;
            }
            return count / elemCount;
        } else if(Predicate.Op.LESS_THAN.equals(op) || Predicate.Op.LESS_THAN_OR_EQ.equals(op)) {
            if(v <= min) {
                return 0.0;
            } else if(v >= max) {
                return 1.0;
            }
            double count = (v - min - step * index) * (double) stats.get(index) / step;
            for(int i = 0; i < index; i++) {
                count += stats.get(i);
            }
            if(Predicate.Op.LESS_THAN_OR_EQ.equals(op)) {
                return count / elemCount + (double)stats.get(index) / step / elemCount;
            }
            return count / elemCount;
        }
        return -1.0;
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
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < stats.size(); i++) {
            stringBuilder.append(min + i * step).append( " - ").append(min + (i + 1) * step)
                    .append(":").append(stats.get(i)).append("\n");
        }

        return stringBuilder.toString();
    }
}
