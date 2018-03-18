package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements BlockOperator {

	private BlockOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] childBlock = child.execute();
		Object result = aggregate(childBlock, fieldNo);

		return new DBColumn[] { new DBColumn(new Object[] { result }, dt) };
	}

	public Object aggregate(DBColumn[] block, int fieldNo) {
		Integer sumInt = new Integer(0);
		Integer maxInt = new Integer(Integer.MIN_VALUE);
		Integer minInt = new Integer(Integer.MAX_VALUE);
		Integer countInt = new Integer(0);
		Double sumDoub = new Double(0);
		Double maxDoub = new Double(Double.MIN_VALUE);
		Double minDoub = new Double(Double.MAX_VALUE);
		Double countDoub = new Double(0);

		switch (block[fieldNo].getDataType()) {
		case INT:
			Integer[] blockAsInt = block[fieldNo].getAsInteger();
			countInt = blockAsInt.length;
			for (Integer val : blockAsInt) {
				sumInt += val;
				maxInt = getMax(maxInt, val);
				minInt = getMin(minInt, val);
			}
			break;
		case DOUBLE:
			Double[] blockAsDouble = block[fieldNo].getAsDouble();
			countDoub = (Double) ((double) blockAsDouble.length);
			for (Double val : blockAsDouble) {
				sumDoub += val;
				maxDoub = getMax(maxDoub, val);
				minDoub = getMin(minDoub, val);
			}
			break;
		case STRING:
			String[] blockAsStrings = block[fieldNo].getAsString();
			countInt = new Integer(blockAsStrings.length);
			break;
		case BOOLEAN:
			Boolean[] blockAsBoolean = block[fieldNo].getAsBoolean();
			countInt = new Integer(blockAsBoolean.length);
			break;
		}

		switch (agg) {
		case COUNT:
			switch (dt) {
			case INT:
				return countInt;
			case DOUBLE:
				return countDoub;
			default:
				return null;
			}
		case SUM:
			switch (dt) {
			case INT:
				return sumInt;
			case DOUBLE:
				return sumDoub;
			default:
				return null;
			}
		case MAX:
			switch (dt) {
			case INT:
				return maxInt;
			case DOUBLE:
				return maxDoub;
			default:
				return null;
			}
		case MIN:
			switch (dt) {
			case INT:
				return minInt;
			case DOUBLE:
				return minDoub;
			default:
				return null;
			}
		case AVG:
			if (countInt != 0) {
				return (Double) ((double) sumInt / (double) countInt);
			} else {
				return sumDoub / countDoub;
			}
		default:
			return null;
		}
	}

	public Double getMin(Double a, Double b) {
		if (a < b) {
			return a;
		} else {
			return b;
		}
	}

	public Integer getMin(Integer a, Integer b) {
		if (a < b) {
			return a;
		} else {
			return b;
		}
	}

	public Double getMax(Double a, Double b) {
		if (a > b) {
			return a;
		} else {
			return b;
		}
	}

	public Integer getMax(Integer a, Integer b) {
		if (a > b) {
			return a;
		} else {
			return b;
		}
	}
}
