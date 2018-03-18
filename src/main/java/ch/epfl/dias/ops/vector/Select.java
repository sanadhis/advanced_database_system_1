package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

public class Select implements VectorOperator {

	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
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
	public DBColumn[] next() {
		DBColumn[] childVector = child.next();
		if (childVector == null) {
			return null;
		} else {
			DBColumn[] vectorSelection = new DBColumn[childVector.length];
			ArrayList<Integer> selectionIndex = new ArrayList<Integer>();
			int index = 0;
			for (Integer val : childVector[fieldNo].getAsInteger()) {
				if (val == null) {
					break;
				}
				switch (op) {
				case LT:
					if (val < value) {
						selectionIndex.add(index);
					}
					break;
				case LE:
					if (val <= value) {
						selectionIndex.add(index);
					}
					break;
				case EQ:
					if (val == value) {
						selectionIndex.add(index);
					}
					break;
				case NE:
					if (val != value) {
						selectionIndex.add(index);
					}
					break;
				case GT:
					if (val > value) {
						selectionIndex.add(index);
					}
					break;
				case GE:
					if (val >= value) {
						selectionIndex.add(index);
					}
					break;
				}
				index++;
			}
			if (selectionIndex.size() != 0) {
				index = 0;
				for (DBColumn block : childVector) {
					Object[] result = null;
					switch (block.getDataType()) {
					case INT:
						result = block.getAsInteger();
						break;
					case DOUBLE:
						result = block.getAsDouble();
						break;
					case STRING:
						result = block.getAsString();
						break;
					case BOOLEAN:
						result = block.getAsBoolean();
						break;
					}
					Object[] blockResult = new Object[selectionIndex.size()];
					for (int i = 0; i < selectionIndex.size(); i++) {
						blockResult[i] = result[selectionIndex.get(i)];
					}
					vectorSelection[index++] = new DBColumn(blockResult, block.getDataType());
				}
				return vectorSelection;
			} else {
				return this.next();
			}

		}
	}

	@Override
	public void close() {
		child.close();
	}
}
