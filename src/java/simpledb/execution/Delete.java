package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private TransactionId t;
    private Tuple rTuple;
    private TupleDesc desc;
    private boolean used;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;
        used = true;
        desc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        used = false;
    }

    public void close() {
       super.close();
       child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        used = false;
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
        if(used) {
            return null;
        }
        if(rTuple == null) {
            int count = 0;
            while(child.hasNext()) {
                Tuple tuple = child.next();
                try {
                    Database.getBufferPool().deleteTuple(t, tuple);
                    count++;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            rTuple = new Tuple(desc);
            rTuple.setField(0, new IntField(count));
        }
        used = true;
        return rTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] opIterators = new OpIterator[1];
        opIterators[0] = child;
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
