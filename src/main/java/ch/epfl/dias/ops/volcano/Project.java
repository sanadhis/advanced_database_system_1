package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	private VolcanoOperator child;
	private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple currentTuple = child.next();
		if (currentTuple.eof){
			return currentTuple;
		}
		Object[] selectedField = new Object[fieldNo.length];
		DataType[] selectedDataType = new DataType[fieldNo.length];
		int index = 0;
		for(int attribute: fieldNo){
			selectedField[index] = currentTuple.getFieldAsObject(attribute);
			selectedDataType[index++] = currentTuple.types[attribute];
		}

		return new DBTuple(selectedField, selectedDataType);
	}

	@Override
	public void close() {
		child.close();
	}
}
