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
	private int fieldNo;
	private DBTuple currentTuple;	

	public Project(VolcanoOperator child, int fieldNo) {
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
		Object selectedField = null;
		DataType selectedType = currentTuple.types[fieldNo];
		switch(selectedType){
			case INT:
				selectedField = currentTuple.getFieldAsInt(fieldNo);
				break;
			case DOUBLE:
				selectedField = currentTuple.getFieldAsDouble(fieldNo);
				break;
			case STRING:
				selectedField = currentTuple.getFieldAsString(fieldNo);
				break;
			case BOOLEAN:
				selectedField = currentTuple.getFieldAsBoolean(fieldNo);
				break;
		}
		return new DBTuple(new Object[]{selectedField}, new DataType[]{selectedType});
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
}
