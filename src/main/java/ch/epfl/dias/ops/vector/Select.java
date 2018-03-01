package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements VectorOperator {

	// TODO: Add required structures

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
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
