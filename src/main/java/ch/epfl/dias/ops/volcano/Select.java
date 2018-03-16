package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple currentTuple = child.next();

		if(currentTuple.eof){
			return currentTuple;
		}

		boolean selection=false;
		try{
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
		}
		catch(NullPointerException e){
			return new DBTuple();
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
		child.close();
	}

}
