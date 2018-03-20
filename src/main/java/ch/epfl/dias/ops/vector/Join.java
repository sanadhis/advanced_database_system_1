package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Join implements VectorOperator {

    private VectorOperator leftChild;
    private VectorOperator rightChild;
    private int leftFieldNo;
    private int rightFieldNo;
    private ArrayList<DBColumn[]> leftVector;
    private ArrayList<Hashtable<Integer, ArrayList<Integer>>> leftHtable;
    private Iterator<DBColumn[]> leftIterator;
    private Iterator<Hashtable<Integer, ArrayList<Integer>>> leftHtableIterator;
    private DBColumn[] currentRightVector;

    private List<List<Object>> selectedLeftFields;
    private List<List<Object>> selectedRightFields;

    private List<List<Object>> savedSelectedLeftFields;
    private List<List<Object>> savedSelectedRightFields;

    private int vectorSize;
    private int numberOfLeftColumns;
    private DataType[] leftTypes;
    private int numberOfRightColumns;
    private DataType[] rightTypes;

    public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftFieldNo = leftFieldNo;
        this.rightFieldNo = rightFieldNo;
        this.leftVector = new ArrayList<DBColumn[]>();
        this.leftHtable = new ArrayList<Hashtable<Integer, ArrayList<Integer>>>();
        this.selectedLeftFields = new ArrayList<List<Object>>();
        this.selectedRightFields = new ArrayList<List<Object>>();
    }

    @Override
    public void open() {
        leftChild.open();
        rightChild.open();

        DBColumn[] currentLeftVector = leftChild.next();
        currentRightVector = rightChild.next();

        try {
            vectorSize = currentLeftVector[leftFieldNo].getAsInteger().length;
            numberOfLeftColumns = currentLeftVector.length;
            leftTypes = new DataType[numberOfLeftColumns];
            int index = 0;
            for (DBColumn column : currentLeftVector) {
                leftTypes[index++] = column.getDataType();
            }
        } catch (NullPointerException e) {
            vectorSize = 0;
        }

        try {
            if (currentRightVector[rightFieldNo].getAsInteger().length > vectorSize) {
                vectorSize = currentRightVector[rightFieldNo].getAsInteger().length;
            }
            numberOfRightColumns = currentRightVector.length;
            rightTypes = new DataType[numberOfRightColumns];
            int index = 0;
            for (DBColumn column : currentRightVector) {
                rightTypes[index++] = column.getDataType();
            }
        } catch (NullPointerException e) {
            vectorSize = 0;
        }

        initSelectedFields(true, true);

        while (currentLeftVector != null) {
            Hashtable<Integer, ArrayList<Integer>> hTable = new Hashtable<Integer, ArrayList<Integer>>();
            int index = 0;
            for (Integer val : currentLeftVector[leftFieldNo].getAsInteger()) {
                if (val != null) {
                    try {
                        hTable.get(val).add(index);
                    } catch (NullPointerException e) {
                        hTable.put(val, new ArrayList<Integer>());
                        hTable.get(val).add(index);
                    }
                    index++;
                }
            }
            leftHtable.add(hTable);
            leftVector.add(currentLeftVector);
            currentLeftVector = leftChild.next();
        }
        leftIterator = leftVector.iterator();
        leftHtableIterator = leftHtable.iterator();
    }

    @Override
    public DBColumn[] next() {

        DBColumn[] currentLeftVector = null;
        Hashtable<Integer, ArrayList<Integer>> currentLeftHTable = new Hashtable<Integer, ArrayList<Integer>>();

        while (currentRightVector != null) {
            if (!leftIterator.hasNext()) {
                currentRightVector = rightChild.next();
                leftIterator = leftVector.iterator();
                leftHtableIterator = leftHtable.iterator();
            }

            try {
                currentLeftVector = leftIterator.next();
                currentLeftHTable = leftHtableIterator.next();
            } catch (NoSuchElementException e) {
                return null;
            }
            Hashtable<Integer, ArrayList<Integer>> rightHtable = new Hashtable<Integer, ArrayList<Integer>>();
            ArrayList<Integer> leftMatchingEntries = new ArrayList<Integer>();
            ArrayList<Integer> rightMatchingEntries = new ArrayList<Integer>();


            if(currentRightVector==null){
                break;
            }

            /*
                Form list of Right elements
            */
            int index = 0;
            for (Integer val : currentRightVector[rightFieldNo].getAsInteger()) {
                if (val != null) {
                    try {
                        ArrayList<Integer> matchingLeft = currentLeftHTable.get(val);
                        if (matchingLeft != null) {
                            for (int i = 0; i < matchingLeft.size(); i++) {
                                rightMatchingEntries.add(index);
                            }
                        }
                        rightHtable.get(val).add(index);
                    } catch (NullPointerException e) {
                        rightHtable.put(val, new ArrayList<Integer>());
                        rightHtable.get(val).add(index);
                    }
                    index++;
                }
            }

            if (rightMatchingEntries.size() != 0) {

                DBColumn[] joinVector = null;

                /*
                    Form list of Left elements
                */
                index = 0;
                for (Integer val : currentLeftVector[leftFieldNo].getAsInteger()) {
                    try {
                        ArrayList<Integer> matchingRight = rightHtable.get(val);
                        if (matchingRight != null) {
                            for (int i = 0; i < matchingRight.size(); i++) {
                                leftMatchingEntries.add(index);
                            }
                        }
                    } catch (NullPointerException e) {
                        // Do nothing
                    }
                    index++;
                }

                index = 0;
                for (DBColumn dbcolumn : currentLeftVector) {
                    Object[] block = null;
                    switch (dbcolumn.getDataType()) {
                    case INT:
                        block = dbcolumn.getAsInteger();
                        break;
                    case DOUBLE:
                        block = dbcolumn.getAsDouble();
                        break;
                    case STRING:
                        block = dbcolumn.getAsString();
                        break;
                    case BOOLEAN:
                        block = dbcolumn.getAsBoolean();
                        break;
                    }
                    for (int i = 0; i < leftMatchingEntries.size(); i++) {
                        selectedLeftFields.get(index).add(block[leftMatchingEntries.get(i)]);
                        if (selectedLeftFields.get(index).size() == vectorSize && (index + 1) == numberOfLeftColumns) {
                            saveSelectedFields(true, false);
                            initSelectedFields(true, false);
                        }
                    }
                    index++;
                }
                index = 0;
                for (DBColumn dbcolumn : currentRightVector) {
                    Object[] block = null;
                    switch (dbcolumn.getDataType()) {
                    case INT:
                        block = dbcolumn.getAsInteger();
                        break;
                    case DOUBLE:
                        block = dbcolumn.getAsDouble();
                        break;
                    case STRING:
                        block = dbcolumn.getAsString();
                        break;
                    case BOOLEAN:
                        block = dbcolumn.getAsBoolean();
                        break;
                    }
                    for (int i = 0; i < rightMatchingEntries.size(); i++) {
                        selectedRightFields.get(index).add(block[rightMatchingEntries.get(i)]);
                        if (selectedRightFields.get(index).size() == vectorSize
                                && (index + 1) == numberOfRightColumns) {
                            saveSelectedFields(false, true);
                            joinVector = formJoinVector();
                            initSelectedFields(false, true);
                        }
                    }
                    index++;
                }

                if (joinVector != null) {
                    return joinVector;
                }
            }
        }

        if (selectedLeftFields.get(0).size() != 0) {
            saveSelectedFields(true, true);
            DBColumn[] joinVec = formJoinVector();
            initSelectedFields(true, true);
            return joinVec;
        } else {
            return null;
        }
    }

    @Override
    public void close() {
        leftChild.close();
        rightChild.close();
    }

    public void initSelectedFields(Boolean left, Boolean right) {
        if (left) {
            selectedLeftFields = new ArrayList<List<Object>>();
            for (int i = 0; i < numberOfLeftColumns; i++) {
                selectedLeftFields.add(new ArrayList<Object>());
            }
        }
        if (right) {
            selectedRightFields = new ArrayList<List<Object>>();
            for (int i = 0; i < numberOfRightColumns; i++) {
                selectedRightFields.add(new ArrayList<Object>());
            }
        }
    }

    public void saveSelectedFields(Boolean left, Boolean right) {
        if (left) {
            savedSelectedLeftFields = new ArrayList<List<Object>>(selectedLeftFields);
        }
        if (right) {
            savedSelectedRightFields = new ArrayList<List<Object>>(selectedRightFields);
        }
    }

    public DBColumn[] formJoinVector() {
        DBColumn[] vectorSelection = new DBColumn[numberOfLeftColumns + numberOfRightColumns];
        int index = 0;
        for (List<Object> perColumnData : savedSelectedLeftFields) {
            Object[] perColumnArray = new Object[perColumnData.size()];
            perColumnArray = perColumnData.toArray(perColumnArray);
            vectorSelection[index] = new DBColumn(perColumnArray, leftTypes[index]);
            index++;
        }
        int index1 = 0;
        for (List<Object> perColumnData : savedSelectedRightFields) {
            Object[] perColumnArray = new Object[perColumnData.size()];
            perColumnArray = perColumnData.toArray(perColumnArray);
            vectorSelection[index] = new DBColumn(perColumnArray, rightTypes[index1++]);
            index++;
        }
        return vectorSelection;
    }
}
