package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

import java.util.Arrays;

public class DBColumn {

    private Object[] fields;
    private DataType type;

    public DBColumn(Object[] fields, DataType type) {
        this.fields = fields;
        this.type = type;
    }

    public DataType getDataType() {
        return this.type;
    }

    public Integer[] getAsInteger() {
        Integer integerArray[] = Arrays.asList(fields).toArray(new Integer[0]);
        return integerArray;
    }

    public Double[] getAsDouble() {
        Double doubleArray[] = Arrays.asList(fields).toArray(new Double[0]);
        return doubleArray;
    }

    public Boolean[] getAsBoolean() {
        Boolean booleanArray[] = Arrays.asList(fields).toArray(new Boolean[0]);
        return booleanArray;
    }

    public String[] getAsString() {
        String stringArray[] = Arrays.asList(fields).toArray(new String[0]);
        return stringArray;
    }
}
