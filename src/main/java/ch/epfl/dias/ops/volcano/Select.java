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

		if (currentTuple.eof) {
			return currentTuple;
		} else {
			boolean selectTuple;
			try {
				int currentTupleFieldValue = currentTuple.getFieldAsInt(fieldNo);
				selectTuple = select(op, currentTupleFieldValue, value);
			} catch (NullPointerException e) {
				return new DBTuple();
			}

			if (selectTuple) {
				return currentTuple;
			} else {
				return this.next();
			}
		}
	}

	@Override
	public void close() {
		child.close();
	}

	public boolean select(BinaryOp op, int currentValue, int selectValue) {
		switch (op) {
		case LT:
			if (currentValue < selectValue) {
				return true;
			}
			return false;
		case LE:
			if (currentValue <= selectValue) {
				return true;
			}
			return false;
		case EQ:
			if (currentValue == selectValue) {
				return true;
			}
			return false;
		case NE:
			if (currentValue != selectValue) {
				return true;
			}
			return false;
		case GT:
			if (currentValue > selectValue) {
				return true;
			}
			return false;
		case GE:
			if (currentValue >= selectValue) {
				return true;
			}
			return false;
		default:
			return false;
		}
	}

}
