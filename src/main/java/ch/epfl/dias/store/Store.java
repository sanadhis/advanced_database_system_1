package ch.epfl.dias.store;

import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

/**
 * This class abstracts the functionality for 
 * Storage models (NSM, DSM and PAX)
 *
 */
public abstract class Store {

	/**
	 * Load the data into the data structures of the store
	 */
	public abstract void load();

	/**
	 * Method to access rows available only for row store and PAX
	 * 
	 * @param rownumber
	 * @return
	 */
	public DBTuple getRow(int rownumber) {
		return null;
	};

	/**
	 * Method to access columns available only for column store
	 * 
	 * @param columnsToGet
	 *            (the set of columns to get)
	 * @return
	 */
	public DBColumn[] getColumns(int[] columnsToGet) {
		return null;
	};
}
