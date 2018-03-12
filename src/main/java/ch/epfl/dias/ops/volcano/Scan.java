package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	// TODO: Add required structures
	private Store store;
	private int rowIndex;

	public Scan(Store store) {
		// TODO: Implement
		this.store = store;
	}

	@Override
	public void open() {
		// TODO: Implement
		rowIndex = 0;
		// store.load();
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		// System.out.println(rowIndex);
		return store.getRow(rowIndex++);
	}

	@Override
	public void close() {
		// TODO: Implement
		rowIndex = 0;
		store = null;
	}

}