package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private Store store;
	private int rowIndex;

	public Scan(Store store) {
		this.store = store;
	}

	@Override
	public void open() {
		rowIndex = 0;
	}

	@Override
	public DBTuple next() {
		return store.getRow(rowIndex++);
	}

	@Override
	public void close() {
		rowIndex = 0;
	}

}