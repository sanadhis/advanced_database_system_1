package ch.epfl.dias.ops.block;

import java.util.ArrayList;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements BlockOperator {

	private BlockOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(BlockOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] childBlock = child.execute();
		DBColumn[] newBlockResult = new DBColumn[childBlock.length];
		ArrayList<Integer> selectionIndex = new ArrayList<Integer>();
		int index = 0;
		for (Integer val : childBlock[fieldNo].getAsInteger()) {
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
		index = 0;
		for (DBColumn block : childBlock) {
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
			newBlockResult[index++] = new DBColumn(blockResult, block.getDataType());
		}

		return newBlockResult;
	}
}
