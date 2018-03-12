package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.block.BlockOperator;
import ch.epfl.dias.store.column.DBColumn;

public class Project implements BlockOperator {

	// TODO: Add required structures
	private BlockOperator childOperator;
	private int[] columns;

	public Project(BlockOperator child, int[] columns) {
		// TODO: Implement
		this.childOperator = child;
		this.columns = columns;
	}

	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] childBlock = childOperator.execute();
		DBColumn[] results = new DBColumn[columns.length];
		int index=0;
		for (int column : columns){
			results[index++] = childBlock[column];
		}
		return results;
	}
}
