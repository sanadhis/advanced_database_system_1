package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	private Integer sumInt;
	private Integer minInt;
	private Integer maxInt;
	private Integer countInt;
	private Double sumDouble;
	private Double minDouble;
	private Double maxDouble;
	private Double countDouble;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
		this.sumInt = new Integer(0);
		this.minInt = new Integer(Integer.MAX_VALUE);
		this.maxInt = new Integer(Integer.MIN_VALUE);
		this.countInt = new Integer(0);
		this.sumDouble = new Double(0);
		this.minDouble = new Double(Double.MAX_VALUE);
		this.maxDouble = new Double(Double.MIN_VALUE);
		this.countDouble = new Double(0);
	}

	@Override
	public void open() {
		child.open();
		DBTuple currentTuple = child.next();
		while (!currentTuple.eof) {
			if (currentTuple != null) {
				aggregate(currentTuple, fieldNo);
			}
			currentTuple = child.next();
		}
	}

	@Override
	public DBTuple next() {
		Object result = aggregateResult(dt, agg);
		return new DBTuple(new Object[] { result }, new DataType[] { dt });
	}

	@Override
	public void close() {
		child.close();
	}

	public void aggregate(DBTuple currentTuple, int fieldNo) {
		switch (currentTuple.types[fieldNo]) {
		case INT:
			Integer currentValueInt = currentTuple.getFieldAsInt(fieldNo);
			countInt += 1;
			sumInt += currentValueInt;
			minInt = getMin(minInt, currentValueInt);
			maxInt = getMax(maxInt, currentValueInt);
			break;
		case DOUBLE:
			Double currentValueDouble = currentTuple.getFieldAsDouble(fieldNo);
			countDouble += 1;
			sumDouble += currentValueDouble;
			minDouble = getMin(minDouble, currentValueDouble);
			maxDouble = getMax(maxDouble, currentValueDouble);
			break;
		case BOOLEAN:
			countInt += 1;
			break;
		case STRING:
			countInt += 1;
			break;
		}
	}

	public Object aggregateResult(DataType type, Aggregate aggregationType) {
		switch (type) {
		case INT:
			switch (aggregationType) {
			case COUNT:
				return countInt;
			case SUM:
				return sumInt;
			case MAX:
				return maxInt;
			case MIN:
				return minInt;
			case AVG:
				return sumInt / countInt;
			default:
				return null;
			}
		case DOUBLE:
			switch (agg) {
			case COUNT:
				return countDouble;
			case SUM:
				return sumDouble;
			case MAX:
				return maxDouble;
			case MIN:
				return minDouble;
			case AVG:
				return sumDouble / countDouble;
			default:
				return null;
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
