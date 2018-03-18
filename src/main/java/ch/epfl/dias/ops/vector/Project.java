package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements VectorOperator {

	private VectorOperator child;
	private int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] childVector = child.next();
		if (childVector == null) {
			return null;
		} else {
			DBColumn[] projectVector = new DBColumn[fieldNo.length];
			int index = 0;
			for (int columToGet : fieldNo) {
				projectVector[index++] = childVector[columToGet];
			}
			return projectVector;
		}
	}

	@Override
	public void close() {
		child.close();
	}
}
