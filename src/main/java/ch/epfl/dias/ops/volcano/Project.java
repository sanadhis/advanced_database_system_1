package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	// TODO: Add required structures

	public Project(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
	}

	@Override
	public void open() {
		// TODO: Implement
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
	}
}
