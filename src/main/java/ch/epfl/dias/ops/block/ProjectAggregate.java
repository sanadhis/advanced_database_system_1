package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements BlockOperator {

	// TODO: Add required structures
	public BlockOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	private Integer sumInt;
	private Integer maxInt;
	private Integer minInt;
	private Integer countInt;
	private Double sumDoub;
	private Double maxDoub;
	private Double minDoub;
	private Double countDoub;
	
	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] childBlock = child.execute();
		Object result = null;
		switch(childBlock[fieldNo].getDataType()){
			case INT:
				Integer[] blockAsInt = childBlock[fieldNo].getAsInteger();
				sumInt = new Integer(0);
				maxInt = new Integer(Integer.MAX_VALUE);
				minInt = new Integer(Integer.MIN_VALUE);
				countInt = new Integer(0);
				for(Integer val: blockAsInt){
					sumInt += val;
					maxInt = getMax(maxInt, val);
					minInt = getMin(minInt, val);
					countInt++;
				}
				break;
			case DOUBLE:
				Double[] blockAsDouble = childBlock[fieldNo].getAsDouble();
				sumDoub = new Double(0);
				maxDoub = new Double(Double.MAX_VALUE);
				minDoub = new Double(Double.MIN_VALUE);
				countDoub = new Double(0);
				for(Double val: blockAsDouble){
					sumDoub += val;
					maxDoub = getMax(maxDoub, val);
					minDoub = getMin(minDoub, val);
					countDoub++;
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
