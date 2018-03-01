package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VectorOperator {

	// TODO: Add required structures

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
	}

	@Override
	public void open() {
		// TODO: Implement
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}

}
