package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.DBColumn;

public interface BlockOperator {
	
	/**
	 * This method invokes the execution of the block-at-a-time operator
	 * 
	 * @return each operator returns the set of result columns
	 */
	public DBColumn[] execute();

}
