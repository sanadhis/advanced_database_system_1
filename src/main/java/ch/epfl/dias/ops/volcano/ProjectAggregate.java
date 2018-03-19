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
    private Double sumDoub;
    private Double minDoub;
    private Double maxDoub;
    private Double countDoub;

    public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
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
        Object result = getAggregateResult(dt, agg);
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
            if (currentValueInt != null) {
                countInt += 1;
                sumInt += currentValueInt;
                minInt = getMin(minInt, currentValueInt);
                maxInt = getMax(maxInt, currentValueInt);
            }
            break;
        case DOUBLE:
            Double currentValueDouble = currentTuple.getFieldAsDouble(fieldNo);
            if (currentValueDouble != null) {
                countDoub += 1;
                sumDoub += currentValueDouble;
                minDoub = getMin(minDoub, currentValueDouble);
                maxDoub = getMax(maxDoub, currentValueDouble);
            }
            break;
        case BOOLEAN:
            Boolean currentValueBoolean = currentTuple.getFieldAsBoolean(fieldNo);
            if (currentValueBoolean != null) {
                countInt += 1;
            }
            break;
        case STRING:
            String currentValueString = currentTuple.getFieldAsString(fieldNo);
            if (currentValueString != null) {
                countInt += 1;
            }
            break;
        }
    }

    public Object getAggregateResult(DataType type, Aggregate aggregationType) {
        switch (aggregationType) {
        case COUNT:
            switch (type) {
            case INT:
                return countInt;
            case DOUBLE:
                return countDoub;
            default:
                return null;
            }
        case SUM:
            switch (type) {
            case INT:
                return sumInt;
            case DOUBLE:
                return sumDoub;
            default:
                return null;
            }
        case MAX:
            switch (type) {
            case INT:
                return maxInt;
            case DOUBLE:
                return maxDoub;
            default:
                return null;
            }
        case MIN:
            switch (type) {
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
