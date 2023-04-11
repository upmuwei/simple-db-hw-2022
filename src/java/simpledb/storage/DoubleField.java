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

    public String toString() {
        return Double.toString(value);
    }

    public int hashCode() {
        return value.hashCode();
    }

    public boolean equals(Object field) {
        if (!(field instanceof DoubleField)) return false;
        return ((DoubleField) field).value.equals(value);
    }

    @Override
    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeDouble(value);
    }

    @Override
    public boolean compare(Predicate.Op op, Field val) {
        if(!(val instanceof DoubleField)) {
            throw new IllegalCastException("val is not an DoubleField");
        }
        DoubleField iVal = (DoubleField) val;

        switch (op) {
            case EQUALS:
            case LIKE:
                return value.equals(iVal.value);
            case NOT_EQUALS:
                return !value.equals(iVal.value);
            case GREATER_THAN:
                return value > iVal.value;
            case GREATER_THAN_OR_EQ:
                return value >= iVal.value;
            case LESS_THAN:
                return value < iVal.value;
            case LESS_THAN_OR_EQ:
                return value <= iVal.value;
        }

        return false;
    }

    @Override
    public Type getType() {
        return Type.DOUBLE_TYPE;
    }
}
