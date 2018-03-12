package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	private DBTuple currentTuple;
	private Integer sumInt;
	private Integer minInt;
	private Integer maxInt;
	private Integer countInt;
	private Integer resultInt;
	private Double sumDouble;
	private Double minDouble;
	private Double maxDouble;
	private Double countDouble;
	private Double resultDouble;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
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

	public Double getMin(Double a, Double b){
		if (a<b){
			return a;
		}
		else{
			return b;
		}
	}

	public Integer getMin(Integer a, Integer b){
		if (a<b){
			return a;
		}
		else{
			return b;
		}
	}

	public Double getMax(Double a, Double b){
		if (a>b){
			return a;
		}
		else{
			return b;
		}
	}

	public Integer getMax(Integer a, Integer b){
		if (a>b){
			return a;
		}
		else{
			return b;
		}
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
		currentTuple = child.next();
		while(!currentTuple.eof){
			if(currentTuple != null){
				switch(dt){
					case INT:
						Integer currentValueInt = currentTuple.getFieldAsInt(fieldNo);
						// System.out.println(currentValueInt);
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
				}
			}
			currentTuple = child.next();
		}
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		Object result = null;

		switch(dt){
			case INT:
				switch(agg){
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
						result = sumInt/countInt;
						break;
				}
				break;
			case DOUBLE:
				switch(agg){
					case COUNT:
						result = countDouble;
						break;
					case SUM:
						result = sumDouble;
						break;
					case MAX:
						result = maxDouble;
						break;
					case MIN:
						result = minDouble;
						break;
					case AVG:
						result = sumDouble/countDouble;
						break;
				}
				break;
		}

		return new DBTuple(new Object[]{result}, 
						   new DataType[]{dt});
	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}

}
