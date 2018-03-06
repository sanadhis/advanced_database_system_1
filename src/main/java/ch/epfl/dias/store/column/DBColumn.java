package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	// TODO: Implement
	public Object[] fields;
	public DataType type;
	public boolean eof;

	public DBColumn(Object[] fields, DataType type){
		this.fields = fields;
		this.type = type;
		this.eof = false;
	}

	public DBColumn(){
		this.eof = true;
	}
	
	public Integer[] getAsInteger() {
		Integer integerArray[] = Arrays.asList(fields)
									   .toArray(new Integer[0]);
		return integerArray;
	}

	public Double[] getAsDouble() {
		Double doubleArray[] = Arrays.asList(fields)
									   .toArray(new Double[0]);
		return doubleArray;
	}

	public Boolean[] getAsBoolean() {
		Boolean booleanArray[] = Arrays.asList(fields)
									   .toArray(new Boolean[0]);
		return booleanArray;
	}

	public String[] getAsString() {
		String stringArray[] = Arrays.asList(fields)
									   .toArray(new String[0]);
		return stringArray;
	}
}
