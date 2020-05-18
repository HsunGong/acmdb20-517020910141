package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private int afield;
    private Op what;

    private TupleDesc tupleDesc;

    private HashMap<Field, Integer> valueMap; // get value of a field
    private HashMap<Field, Integer> countMap;  // Get counts of a field

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;

        if (gbfield == Aggregator.NO_GROUPING)
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
        else
            this.tupleDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, new String[]{"groupFieldValue", "aggregateValue"});
        
        valueMap = new HashMap<Field, Integer>();
        countMap = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupfield = (gbfield == Aggregator.NO_GROUPING) ? null : tup.getField(gbfield);

        Integer oldvalue = valueMap.get(groupfield); // might be null(no group)
        Integer curvalue = ((IntField) tup.getField(afield)).getValue();
        Integer newvalue = null;

        switch (this.what){ // HashMap make do not exist as null;
            case MIN:
                newvalue = (oldvalue == null) ? curvalue : Integer.min(oldvalue, curvalue);
                break;
                case MAX:
                newvalue = (oldvalue == null) ? curvalue : Integer.max(oldvalue, curvalue);
                break;
                case SUM:
                newvalue = (oldvalue == null) ? curvalue : Integer.sum(oldvalue, curvalue);
                break;
            case COUNT:
                newvalue = (oldvalue == null) ? 1 : oldvalue + 1;
                break;
            case AVG:
                newvalue = (oldvalue == null) ? curvalue : curvalue + oldvalue;
                // counting + 1
                countMap.put(groupfield, countMap.getOrDefault(groupfield, 0) + 1);
                break;
            default:
                assert false : "Unsupported Aggregator.Operators";
        }

        valueMap.put(groupfield, newvalue);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        ArrayList <Tuple> tuples = new ArrayList<Tuple>();

        for (HashMap.Entry<Field, Integer> entry : valueMap.entrySet()){
            Tuple tuple = new Tuple(tupleDesc);
            Integer value = entry.getValue();
            if (this.what == Op.AVG) value = value / countMap.get(entry.getKey());
            
            if (gbfield == Aggregator.NO_GROUPING)
                tuple.setField(0, new IntField(value));
            else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(value));
            }
            
            tuples.add(tuple);
        }

        return new TupleIterator(tupleDesc, tuples);
    }

}
