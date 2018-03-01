package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.column.DBColumn;

public interface VectorOperator {

	/**
	 * Open operation for vector-at-a-time operator
	 */
	void open();

	/**
	 * Fetch next tuple for vector-at-a-time operator
	 * 
	 * @return
	 */
	DBColumn[] next();

	/**
	 * Close operation for vector-at-a-time operator
	 */
	void close();
}
