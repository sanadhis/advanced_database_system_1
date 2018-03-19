package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.List;

public class Select implements VectorOperator {

    private VectorOperator child;
    private BinaryOp op;
    private int fieldNo;
    private int value;
    private int vectorSize;
    private DBColumn[] childVector;
    List<List<Object>> selectedFields;

    private int accumulatedEntry;
    private int numberOfColumns;
    private DataType[] types;

    public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
        this.child = child;
        this.op = op;
        this.fieldNo = fieldNo;
        this.value = value;
        this.selectedFields = new ArrayList<List<Object>>();
        this.numberOfColumns = 0;
        this.types = new DataType[0];
    }

    @Override
    public void open() {
        child.open();
        childVector = child.next();
        try {
            initSelectedFields();
            vectorSize = childVector[fieldNo].getAsInteger().length;
            numberOfColumns = childVector.length;
            types = new DataType[numberOfColumns];
            int index = 0;
            for (DBColumn column : childVector) {
                types[index++] = column.getDataType();
            }
        } catch (NullPointerException e) {
            vectorSize = 0;
        }
    }

    @Override
    public DBColumn[] next() {
        if (vectorSize == 0 || childVector == null) {
            if (accumulatedEntry != 0) {
                accumulatedEntry = 0;
                return this.formSelectedVector();
            } else {
                return null;
            }
        } else {
            int index = 0;
            ArrayList<Integer> selectionIndex = new ArrayList<Integer>();
            DBColumn[] vectorSelection = null;
            for (Integer val : childVector[fieldNo].getAsInteger()) {
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
                if ((accumulatedEntry + selectionIndex.size()) == vectorSize) {
                    formSelectedFields(selectionIndex);
                    vectorSelection = formSelectedVector();
                    initSelectedFields();
                    selectionIndex = new ArrayList<Integer>();
                }
                index++;
            }
            formSelectedFields(selectionIndex);
            selectionIndex = new ArrayList<Integer>();
            childVector = child.next();

            if (vectorSelection != null) {
                return vectorSelection;
            } else {
                return this.next();
            }
        }

    }

    @Override
    public void close() {
        child.close();
    }

    public void formSelectedFields(ArrayList<Integer> selectionIndex) {
        if (selectionIndex.size() != 0) {
            int columnIndex = 0;
            for (DBColumn block : childVector) {
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
                for (int i = 0; i < selectionIndex.size(); i++) {
                    selectedFields.get(columnIndex).add(result[selectionIndex.get(i)]);
                }
                columnIndex++;
            }
            accumulatedEntry += selectionIndex.size();
        }
    }

    public DBColumn[] formSelectedVector() {
        DBColumn[] vectorSelection = new DBColumn[numberOfColumns];
        int index = 0;
        for (List<Object> perColumnData : selectedFields) {
            Object[] perColumnArray = new Object[perColumnData.size()];
            perColumnArray = perColumnData.toArray(perColumnArray);
            vectorSelection[index] = new DBColumn(perColumnArray, types[index]);
            index++;
        }
        return vectorSelection;
    }

    public void initSelectedFields() {
        selectedFields = new ArrayList<List<Object>>();
        for (int i = 0; i < childVector.length; i++) {
            selectedFields.add(new ArrayList<Object>());
        }
        accumulatedEntry = 0;
    }
}
