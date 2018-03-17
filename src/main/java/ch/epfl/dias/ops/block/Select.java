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
		DBColumn[] blockResults = new DBColumn[childBlock.length];
		ArrayList<Integer> selectedIndex = getIndexBySelection(childBlock[fieldNo]);
		int index = 0;
		for (DBColumn block : childBlock) {
			Object[] selectedPerBlock = selectPerBlock(block, selectedIndex);
			blockResults[index++] = new DBColumn(selectedPerBlock, block.getDataType());
		}

		return blockResults;
	}

	public ArrayList<Integer> getIndexBySelection(DBColumn block){
		int index = 0;
		ArrayList<Integer> selectedIndex = new ArrayList<Integer>();
		for (Integer val : block.getAsInteger()) {
			switch (op) {
			case LT:
				if (val < value) {
					selectedIndex.add(index);
				}
				break;
			case LE:
				if (val <= value) {
					selectedIndex.add(index);
				}
				break;
			case EQ:
				if (val == value) {
					selectedIndex.add(index);
				}
				break;
			case NE:
				if (val != value) {
					selectedIndex.add(index);
				}
				break;
			case GT:
				if (val > value) {
					selectedIndex.add(index);
				}
				break;
			case GE:
				if (val >= value) {
					selectedIndex.add(index);
				}
				break;
			}
			index++;
		}
		return selectedIndex;
	}

	public Object[] selectPerBlock(DBColumn block, ArrayList<Integer> selectedIndex) {
		Object[] result;
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
		default:
			result = null;
		}
		Object[] selectedPerBlock = new Object[selectedIndex.size()];
		for (int i = 0; i < selectedIndex.size(); i++) {
			selectedPerBlock[i] = result[selectedIndex.get(i)];
		}
		return selectedPerBlock;
	}
}
