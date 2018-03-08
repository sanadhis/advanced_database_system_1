package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements BlockOperator {

	// TODO: Add required structures
	private ColumnStore store;

	public Scan(ColumnStore store) {
		// TODO: Implement
		this.store = store;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		int numberOfColumns = store.getNumberOfColumns();
		int[] columsToGet = new int[numberOfColumns];
		for(int i=0;i<numberOfColumns;i++){
			columsToGet[i] = i;
		}
		return store.getColumns(columsToGet);
	}
}
