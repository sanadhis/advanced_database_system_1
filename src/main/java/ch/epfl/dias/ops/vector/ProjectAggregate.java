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
            aggregate(currentVector, fieldNo);
            currentVector = child.next();
        }
    }

    @Override
    public DBColumn[] next() {
        Object result = getAggregateResult(dt, agg);
        return new DBColumn[] { new DBColumn(new Object[] { result }, dt) };
    }

    @Override
    public void close() {
        child.close();
    }

    public void aggregate(DBColumn[] vector, int fieldNo) {
        switch (vector[fieldNo].getDataType()) {
        case INT:
            Integer[] vectorAsInt = vector[fieldNo].getAsInteger();
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
            Double[] vectorAsDouble = vector[fieldNo].getAsDouble();
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
            String[] vectorAsStrings = vector[fieldNo].getAsString();
            for (String str : vectorAsStrings) {
                if (str == null) {
                    break;
                } else {
                    countInt++;
                }
            }
            break;
        case BOOLEAN:
            Boolean[] vectorAsBoolean = vector[fieldNo].getAsBoolean();
            for (Boolean bool : vectorAsBoolean) {
                if (bool == null) {
                    break;
                } else {
                    countInt++;
                }
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
