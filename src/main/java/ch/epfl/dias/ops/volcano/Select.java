package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	private DBTuple currentTuple;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
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

		if(currentTuple.eof){
			return currentTuple;
		}

		boolean selection=false;
		switch(op){
			case LT:
				if(currentTuple.getFieldAsInt(fieldNo) < value){
					selection = true;
				}
				break;
			case LE:
				if(currentTuple.getFieldAsInt(fieldNo) <= value){
					selection = true;
				}
				break;
			case EQ:
				if(currentTuple.getFieldAsInt(fieldNo) == value){
					selection = true;
				}
				break;
			case NE:
				if(currentTuple.getFieldAsInt(fieldNo) != value){
					selection = true;
				}
				break;
			case GT:
				if(currentTuple.getFieldAsInt(fieldNo) > value){
					selection = true;
				}
				break;
			case GE:
				if(currentTuple.getFieldAsInt(fieldNo) >= value){
					selection = true;
				}
				break;
		}
		if(selection){
			return currentTuple;
		}
		else{
			return this.next();
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}

}
