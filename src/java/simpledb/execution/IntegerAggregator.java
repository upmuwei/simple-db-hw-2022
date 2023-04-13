package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.awt.geom.AffineTransform;
import java.io.File;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Field tField;
    private Map<Field, List<Field>> groupMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field aField = tup.getField(afield);
        if(gbfield != -1) {
            tField = tup.getField(gbfield);
        }
        if(groupMap.get(tField) == null) {
            List<Field> list = new ArrayList<>();
            list.add(aField);
            groupMap.put(tField, list);
            return;
        }
        List<Field> list= groupMap.get(tField);
        list.add(aField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        Type[] t;
        TupleDesc tDesc;
        Tuple[] tuples;
        if(gbfield == -1) {
            List<Field> list = groupMap.get(tField);
            t = new Type[1];
            t[0] = Type.INT_TYPE;
            tDesc = new TupleDesc(t);
            tuples = new Tuple[1];
            tuples[0] = new Tuple(tDesc);
            if(what == Op.MIN) {
                Field f = list.get(0);
                for(Field field : list) {
                    if(field.compare(Predicate.Op.LESS_THAN, f)) {
                        f = field;
                    }
                }
                tuples[0].setField(0, f);
            } else if(what == Op.MAX) {
                Field f = list.get(0);
                for(Field field : list) {
                    if(field.compare(Predicate.Op.GREATER_THAN, f)) {
                        f = field;
                    }
                }
                tuples[0].setField(0, f);
            } else if(what == Op.AVG) {
                int sum = 0;
                for(Field field : list) {
                    sum += ((IntField) field).getValue();
                }
                tuples[0].setField(0, new IntField(sum / list.size()));
            } else if(what == Op.COUNT) {
                tuples[0].setField(0, new IntField(list.size()));
            } else if(what == Op.SUM) {
                int sum = 0;
                for(Field field : list) {
                    sum += ((IntField) field).getValue();
                }
                tuples[0].setField(0, new IntField(sum));
            }
        } else {
            t = new Type[2];
            t[0] = gbfieldtype;
            t[1] = Type.INT_TYPE;
            tDesc = new TupleDesc(t);
            Set<Field> fieldSet = groupMap.keySet();
            tuples = new Tuple[fieldSet.size()];
            int index = 0;
            for(Field kField : fieldSet) {
                tuples[index] = new Tuple(tDesc);
                tuples[index].setField(0, kField);
                List<Field> fieldList = groupMap.get(kField);
                if(what == Op.MIN) {
                    Field f = fieldList.get(0);
                    for(Field field : fieldList) {
                        if(field.compare(Predicate.Op.LESS_THAN, f)) {
                            f = field;
                        }
                    }
                    tuples[index].setField(1, f);
                } else if(what == Op.MAX) {
                    Field f = fieldList.get(0);
                    for(Field field : fieldList) {
                        if(field.compare(Predicate.Op.GREATER_THAN, f)) {
                            f = field;
                        }
                    }
                    tuples[index].setField(1, f);
                } else if(what == Op.AVG) {
                    int sum = 0;
                    for(Field field : fieldList) {
                        sum += ((IntField) field).getValue();
                    }
                    tuples[index].setField(1, new IntField(sum / fieldList.size()));
                } else if(what == Op.COUNT) {
                    tuples[index].setField(1, new IntField(fieldList.size()));
                } else if(what == Op.SUM) {
                    int sum = 0;
                    for(Field field : fieldList) {
                        sum += ((IntField) field).getValue();
                    }
                    tuples[index].setField(1, new IntField(sum));
                }
                index++;
            }

        }
        return new IntegerIterator(tuples, tDesc);
    }

    private class IntegerIterator implements OpIterator {
        private Tuple[] tuples;
        private TupleDesc desc;

        private int index;

        private boolean isOpen;
        public IntegerIterator(Tuple[] tuples, TupleDesc desc) {
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
