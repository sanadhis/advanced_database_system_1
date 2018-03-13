package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.vector.VectorOperator;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator child;
	private int[] fieldNo;
	private DBTuple currentTuple;	

	public Project(VolcanoOperator child, int[] fieldNo) {
		// TODO: Implement
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		currentTuple = child.next();
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
		// TODO: Implement
		child.close();
	}
}
