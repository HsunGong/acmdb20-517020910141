package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min, max;
    
    private int ntups;
    private int width;
    private int[] histogram;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
        this.max = max;
        if (max - min + 1 < buckets) buckets = max - min + 1; //optim
        this.buckets = buckets;

        this.width = (int) Math.ceil((double) (max - min + 1) / buckets); // avoid 0
        this.ntups = 0;

        this.histogram = new int[buckets];
        for (int i = 0; i < this.histogram.length; i++) this.histogram[i] = 0;
    }

    private int val2Idx(int v) {
        return (v == max) ? buckets - 1 : (v - min) / width;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int index = val2Idx(v);
        ++ histogram[index];
        ++ ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int bucketIndex = val2Idx(v);
        int height = (v >= min && v <= max) ? histogram[bucketIndex] : -1;

        int left = bucketIndex * width + min;
        int right = bucketIndex * width + min + width - 1;

        switch (op) {
            case EQUALS:
                if (v < min || v > max) return 0.0;
            
                return (double) height / width / ntups;
            case NOT_EQUALS:
                return (double) 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN:
                if (v < min) return 1.0;
                if (v > max) return 0.0;
                
                // partial s.t. datas in histogram[idx]
                double partialIn = (double) ((right - v) / width) * height;
                // full s.t. datas in his[>idx]
                double allIn = 0;
                for (int i = bucketIndex + 1; i < buckets; ++i) allIn += (double) histogram[i];

                return (partialIn + allIn) / ntups;
            case LESS_THAN:
                if (v < min) return 0.0;
                if (v > max) return 1.0;

                partialIn = (double) ((v - left) / width) * height;
                
                allIn = 0;
                for (int i = bucketIndex - 1; i >= 0; --i) allIn += histogram[i];

                return (partialIn + allIn)/ ntups;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
                // return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
                // return estimateSelectivity(Predicate.Op.GREATER_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            case LIKE: // NOT supported
                return avgSelectivity();
            default:
                throw new RuntimeException("Not such OP");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
