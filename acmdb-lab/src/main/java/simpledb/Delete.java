package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        reset();
    }

    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        assert (child != null): "GG";
        child.open();
        super.open();
        reset();
    }

    public void close() {
        super.close();
        child.close();
        reset();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        open();
        close();
    }

    private int count;
    private boolean called;
    private void reset() {
        count = 0;
        called = false;
    }


    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (child == null || called) return null;
        
        while (child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(tid, child.next());
            } catch (IOException e) {
                e.printStackTrace();
            }
            count ++;
        }
        called = true;

        Tuple ans = new Tuple(getTupleDesc());
        ans.setField(0, new IntField(count));
        return ans;
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
