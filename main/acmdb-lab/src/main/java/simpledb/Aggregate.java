package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child, aggregatorIterator;
    private int afield, gfield;
    private Aggregator.Op aop;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        this.aggregatorIterator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
        return (this.gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        if (child == null) throw new NoSuchElementException("Child is null.");
        child.open();
        super.open();
        aggregatorIterator = null;
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggregatorIterator == null) {
            if (child == null) return null;
            
            Aggregator aggregator = null;
            Type gfieldtype = (gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getFieldType(gfield);
            
            aggregator = (child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE)) ? 
                new IntegerAggregator(gfield, gfieldtype, afield, aop) :
                new StringAggregator(gfield, gfieldtype, afield, aop);

            while (child.hasNext()) aggregator.mergeTupleIntoGroup(child.next());
            aggregatorIterator = aggregator.iterator();
            aggregatorIterator.open();
        }

        if (aggregatorIterator.hasNext()) return aggregatorIterator.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        // super.rewind();
        aggregatorIterator = null;
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        Type[] types;
        String[] fieldnames;
        
        if (gfield == Aggregator.NO_GROUPING) {
            types = new Type[] {Type.INT_TYPE};
            fieldnames = new String[] {aop.toString() + "(" + child.getTupleDesc().getFieldName(afield) + ")"};
        } else {
            types = new Type[] {child.getTupleDesc().getFieldType(gfield), Type.INT_TYPE};
            fieldnames = new String[] {child.getTupleDesc().getFieldName(gfield), aop.toString() + "(" + child.getTupleDesc().getFieldName(afield) + ")"};
        }

        return new TupleDesc(types, fieldnames);
    }

    public void close() {
        super.close();
        child.close();
        aggregatorIterator = null;
    }

    @Override
    public DbIterator[] getChildren() {
	    return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    child = children[0];
    }
    
}
