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
		Integer sumInt = new Integer(0);
		Integer maxInt = new Integer(Integer.MAX_VALUE);
		Integer minInt = new Integer(Integer.MIN_VALUE);
		Integer countInt = new Integer(0);
		Double sumDoub = new Double(0);
		Double maxDoub = new Double(Double.MAX_VALUE);
		Double minDoub = new Double(Double.MIN_VALUE);
		Double countDoub = new Double(0);

		DBColumn[] childBlock = child.execute();
		Object result = null;
		switch(childBlock[fieldNo].getDataType()){
			case INT:
				Integer[] blockAsInt = childBlock[fieldNo].getAsInteger();
				countInt = blockAsInt.length;
				for(Integer val: blockAsInt){
					sumInt += val;
					maxInt = getMax(maxInt, val);
					minInt = getMin(minInt, val);
				}
				break;
			case DOUBLE:
				Double[] blockAsDouble = childBlock[fieldNo].getAsDouble();
				countDoub = (Double)((Object)blockAsDouble.length);
				for(Double val: blockAsDouble){
					sumDoub += val;
					maxDoub = getMax(maxDoub, val);
					minDoub = getMin(minDoub, val);
				}
				break;
			case STRING:
				String[] blockAsStrings = childBlock[fieldNo].getAsString();
				countInt = new Integer(blockAsStrings.length);
				break;
			case BOOLEAN:
				Boolean[] blockAsBoolean = childBlock[fieldNo].getAsBoolean();
				countInt = new Integer(blockAsBoolean.length);
				break;
		}

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
						result = sumDoub/countDoub;
						break;
				}
				break;
		}
		
		return new DBColumn[]{
			new DBColumn(
				new Object[]{result},dt
				)
			};
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
}
