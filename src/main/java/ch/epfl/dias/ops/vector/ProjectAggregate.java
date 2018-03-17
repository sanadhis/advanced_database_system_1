package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements VectorOperator {

	private VectorOperator child;
	private Aggregate agg;
	private int fieldNo;
	private DataType dt;
	private Integer sumInt;
	private Integer minInt;
	private Integer maxInt;
	private Integer countInt;
	private Double sumDoub;
	private Double minDoub;
	private Double maxDoub;
	private Double countDoub;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
		this.sumInt = new Integer(0);
		this.minInt = new Integer(Integer.MAX_VALUE);
		this.maxInt = new Integer(Integer.MIN_VALUE);
		this.countInt = new Integer(0);
		this.sumDoub = new Double(0);
		this.minDoub = new Double(Double.MAX_VALUE);
		this.maxDoub = new Double(Double.MIN_VALUE);
		this.countDoub = new Double(0);
	}

	@Override
	public void open() {
		child.open();
		DBColumn[] currentVector = child.next();
		while (currentVector != null) {
			switch (currentVector[fieldNo].getDataType()) {
			case INT:
				Integer[] vectorAsInt = currentVector[fieldNo].getAsInteger();
				for (Integer val : vectorAsInt) {
					try {
						sumInt += val;
						maxInt = getMax(maxInt, val);
						minInt = getMin(minInt, val);
						countInt++;
					} catch (NullPointerException e) {
						break;
					}
				}
				break;
			case DOUBLE:
				Double[] vectorAsDouble = currentVector[fieldNo].getAsDouble();
				for (Double val : vectorAsDouble) {
					try {
						sumDoub += val;
						maxDoub = getMax(maxDoub, val);
						minDoub = getMin(minDoub, val);
						countDoub++;
					} catch (NullPointerException e) {
						break;
					}
				}
				break;
			case STRING:
				String[] vectorAsStrings = currentVector[fieldNo].getAsString();
				for (String str : vectorAsStrings) {
					if (str == null) {
						break;
					} else {
						countInt++;
					}
				}
				break;
			case BOOLEAN:
				Boolean[] vectorAsBoolean = currentVector[fieldNo].getAsBoolean();
				for (Boolean bool : vectorAsBoolean) {
					if (bool == null) {
						break;
					} else {
						countInt++;
					}
				}
				break;
			}
			currentVector = child.next();
		}
	}

	@Override
	public DBColumn[] next() {
		Object result = null;
		switch (dt) {
		case INT:
			switch (agg) {
			case COUNT:
				result = countInt;
				break;
			case SUM:
				result = sumInt;
				break;
			case MAX:
				result = maxInt;
				break;
			case MIN:
				result = minInt;
				break;
			case AVG:
				result = sumInt / countInt;
				break;
			}
			break;
		case DOUBLE:
			switch (agg) {
			case COUNT:
				result = countDoub;
				break;
			case SUM:
				result = sumDoub;
				break;
			case MAX:
				result = maxDoub;
				break;
			case MIN:
				result = minDoub;
				break;
			case AVG:
				result = sumDoub / countDoub;
				break;
			}
			break;
		}

		return new DBColumn[] { new DBColumn(new Object[] { result }, dt) };
	}

	@Override
	public void close() {
		child.close();
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
