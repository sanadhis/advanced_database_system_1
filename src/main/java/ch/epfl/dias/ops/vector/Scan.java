package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	// TODO: Add required structures
	private Store store;
	private int vectorSize;
	private int vectorIndex;
	private DBColumn[] allColumns;
	private boolean eofFlag;

	public Scan(Store store, int vectorSize) {
		// TODO: Implement
		this.store = store;
		this.vectorSize = vectorSize;
		this.allColumns = null;
		this.eofFlag = false;
	}
	
	@Override
	public void open() {
		// TODO: Implement
		vectorIndex = 0;
		allColumns = store.getColumns(new int[]{});
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		if(eofFlag){
			return null;
		}
		else{
			DBColumn[] nextVector = new DBColumn[allColumns.length];
			int index = 0;
			for(DBColumn column: allColumns){
				Object[] result = null;
				switch(column.getDataType()){
					case INT:
						result = column.getAsInteger();
						break;
					case DOUBLE:
						result = column.getAsDouble();
						break;
					case STRING:
						result = column.getAsString();
						break;
					case BOOLEAN:
						result = column.getAsBoolean();
						break;
				}
				Object[] blockResult = new Object[vectorSize];
				for (int i=0; i<vectorSize; i++){
					try{
						blockResult[i] = result[vectorIndex+i];
					}
					catch(IndexOutOfBoundsException e){
						blockResult[i] = null;
						eofFlag = true;
					}
				}
				nextVector[index++] = new DBColumn(blockResult, column.getDataType());			
			}
			vectorIndex += vectorSize;		
			return nextVector;
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		vectorIndex = 0;
		store = null;
	}
}
