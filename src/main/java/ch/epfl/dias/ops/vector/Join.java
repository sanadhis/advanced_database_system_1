package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

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

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.leftVector = new ArrayList<DBColumn[]>();
		this.leftHtable = new ArrayList<Hashtable<Integer, ArrayList<Integer>>>();
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		DBColumn[] currentLeftVector = leftChild.next();
		while (currentLeftVector != null) {
			Hashtable<Integer, ArrayList<Integer>> hTable = new Hashtable<Integer, ArrayList<Integer>>();
			int index = 0;
			for (Integer val : currentLeftVector[leftFieldNo].getAsInteger()) {
				try {
					hTable.get(val).add(index);
				} catch (NullPointerException e) {
					hTable.put(val, new ArrayList<Integer>());
					hTable.get(val).add(index);
				}
				index++;
			}
			leftHtable.add(hTable);
			leftVector.add(currentLeftVector);
			currentLeftVector = leftChild.next();
		}
		leftIterator = leftVector.iterator();
		leftHtableIterator = leftHtable.iterator();
		currentRightVector = rightChild.next();
	}

	@Override
	public DBColumn[] next() {

		DBColumn[] currentLeftVector = null;
		Hashtable<Integer, ArrayList<Integer>> currentLeftHTable = new Hashtable<Integer, ArrayList<Integer>>();

		if (!leftIterator.hasNext()) {
			currentRightVector = rightChild.next();
			leftIterator = leftVector.iterator();
			leftHtableIterator = leftHtable.iterator();
		}

		currentLeftVector = leftIterator.next();
		currentLeftHTable = leftHtableIterator.next();

		if (currentRightVector == null) {
			return null;
		} else {
			Hashtable<Integer, ArrayList<Integer>> rightHtable = new Hashtable<Integer, ArrayList<Integer>>();
			ArrayList<Integer> leftMatchingEntries = new ArrayList<Integer>();
			ArrayList<Integer> rightMatchingEntries = new ArrayList<Integer>();

			/*
				Form list of Right elements
			*/
			int index = 0;
			for (Integer val : currentRightVector[rightFieldNo].getAsInteger()) {
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

			DBColumn[] joinResult = new DBColumn[currentLeftVector.length + currentRightVector.length];
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
				Object[] blockResult = new Object[leftMatchingEntries.size()];
				for (int i = 0; i < leftMatchingEntries.size(); i++) {
					blockResult[i] = block[leftMatchingEntries.get(i)];
				}
				joinResult[index++] = new DBColumn(blockResult, dbcolumn.getDataType());
			}
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
				Object[] blockResult = new Object[rightMatchingEntries.size()];
				for (int i = 0; i < rightMatchingEntries.size(); i++) {
					blockResult[i] = block[rightMatchingEntries.get(i)];
				}
				joinResult[index++] = new DBColumn(blockResult, dbcolumn.getDataType());
			}
			return joinResult;
		}
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}
}
