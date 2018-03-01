package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.row.DBTuple;

public interface VolcanoOperator {

	/**
	 * Open operation for tuple-at-a-time operator
	 */
	void open();

	/**
	 * Fetch next tuple for tuple-at-a-time operator
	 * 
	 * @return
	 */
	DBTuple next();

	/**
	 * Close operation for tuple-at-a-time operator
	 */
	void close();
}
