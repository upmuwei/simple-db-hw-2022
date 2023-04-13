package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Field tField;
    private Map<Field, Integer> groupMap;
    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if(gbfield == -1) {
            tField = new IntField(-1);
        }
        groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(what == Op.COUNT) {
            if(gbfield != -1) {
                tField = tup.getField(gbfield);
            }
            if(groupMap.get(tField) == null) {
                groupMap.put(tField, 1);
                return;
            }
            int count =  groupMap.get(tField);
            groupMap.put(tField, ++count);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        Type[] t;
        TupleDesc tDesc;
        Tuple[] tuples;
        if(gbfield == -1) {
            t = new Type[1];
            t[0] = Type.INT_TYPE;
            tDesc = new TupleDesc(t);
            tuples = new Tuple[1];
            tuples[0] = new Tuple(tDesc);
            tuples[0].setField(0, new IntField(groupMap.get(tField)));
        } else {
            Set<Field> fieldSet = groupMap.keySet();
            t = new Type[2];
            t[0] = gbfieldtype;
            t[1] = Type.INT_TYPE;
            tDesc = new TupleDesc(t);
            tuples = new Tuple[fieldSet.size()];
            int index = 0;
            for(Field kField : fieldSet) {
                tuples[index] = new Tuple(tDesc);
                tuples[index].setField(0, kField);
                tuples[index++].setField(1, new IntField(groupMap.get(kField)));
            }
        }
        return new StringIterator(tuples, tDesc);
    }

    private class StringIterator implements OpIterator {
        private Tuple[] tuples;
        private TupleDesc desc;

        private int index;

        private boolean isOpen;
        public StringIterator(Tuple[] tuples, TupleDesc desc) {
            this.tuples = tuples;
            this.desc = desc;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            index = 0;
            isOpen = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(!isOpen) {
                throw new IllegalStateException("Operator not yet open");
            }
            return index < tuples.length;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!isOpen) {
                throw new IllegalStateException("Operator not yet open");
            }
            if(index < tuples.length) {
                return tuples[index++];
            }
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if(!isOpen) {
                throw new IllegalStateException("Operator not yet open");
            }
            index = 0;
        }

        @Override
        public TupleDesc getTupleDesc() {
            return desc;
        }

        @Override
        public void close() {
            index = tuples.length;
            isOpen = false;
        }
    }
}
