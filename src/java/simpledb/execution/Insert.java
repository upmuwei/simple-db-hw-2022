package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private TransactionId t;
    private int tableId;
    private TupleDesc desc;
    private Tuple rTuple;
    private boolean used;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        desc = new TupleDesc(new Type[]{Type.INT_TYPE});
        used = true;
    }

    public TupleDesc getTupleDesc() {
        return desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
                    Database.getBufferPool().insertTuple(t, tableId, tuple);
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
