package simpledb.storage;

import simpledb.common.Type;
import simpledb.execution.Predicate;

import java.io.DataOutputStream;
import java.io.IOException;

public class DoubleField implements Field{

    private static final long serialVersionUID = 1L;

    private final Double value;

    public Double getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param i The value of this field.
     */
    public DoubleField(Double i) {
        value = i;
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {

    }

    @Override
    public boolean compare(Predicate.Op op, Field value) {
        return false;
    }

    @Override
    public Type getType() {
        return null;
    }
}
