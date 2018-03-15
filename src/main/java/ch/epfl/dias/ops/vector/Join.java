package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.Iterator;

public class Join implements VectorOperator {

	// TODO: Add required structures
	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private ArrayList<DBColumn[]> leftVector;
	private Iterator<DBColumn[]> leftIterator;
	private DBColumn[] currentRightVector;


	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.leftVector = new ArrayList<DBColumn[]>();		
	}

	@Override
	public void open() {
		// TODO: Implement
		leftChild.open();
		rightChild.open();
		DBColumn[] currentLeftVector = leftChild.next();
		while(currentLeftVector!=null){
			leftVector.add(currentLeftVector);
			currentLeftVector = leftChild.next();			
		}
		leftIterator = leftVector.iterator();
		currentRightVector = rightChild.next();
		System.out.println(leftVector.size());
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement

		DBColumn[] currentLeftVector  = null;

		if(leftIterator.hasNext()){
			currentLeftVector = leftIterator.next();
		}
		else{
			currentRightVector = rightChild.next();
			leftIterator = leftVector.iterator();
			currentLeftVector = leftIterator.next();
		}

		if(currentRightVector==null){
			return null;
		}
		else{
			Hashtable<Integer, ArrayList<Integer>> leftHtable = new Hashtable<Integer, ArrayList<Integer>>();
			Hashtable<Integer, ArrayList<Integer>> rightHtable = new Hashtable<Integer, ArrayList<Integer>>();
			ArrayList<Integer> leftMatchingEntries = new ArrayList<Integer>();
			ArrayList<Integer> rightMatchingEntries = new ArrayList<Integer>();

			int index = 0;
			for (Integer val: currentLeftVector[leftFieldNo].getAsInteger()){
				try{
					leftHtable.get(val).add(index);
				}
				catch(NullPointerException e){
					leftHtable.put(val, new ArrayList<Integer>());
					leftHtable.get(val).add(index);
				}
				index++;
			}

			/*
				Form list of Right elements
			*/
			index = 0;
			for (Integer val: currentRightVector[rightFieldNo].getAsInteger()){
				try{
					ArrayList<Integer> matchingLeft = leftHtable.get(val);
					if (matchingLeft!=null){
						for(int i=0;i<matchingLeft.size();i++){
							rightMatchingEntries.add(index);
						}
					}
					rightHtable.get(val).add(index);
				}
				catch(NullPointerException e){
					rightHtable.put(val, new ArrayList<Integer>());
					rightHtable.get(val).add(index);
				}
				index++;
			}

			/*
				Form list of Left elements
			*/
			index = 0;
			for (Integer val: currentLeftVector[leftFieldNo].getAsInteger()){
				try{
					ArrayList<Integer> matchingRight = rightHtable.get(val);
					if (matchingRight!=null){
						for(int i=0;i<matchingRight.size();i++){
							leftMatchingEntries.add(index);
						}
					}
				}
				catch(NullPointerException e){
					// Do nothing
				}
				index++;
			}

			DBColumn[] joinResult = new DBColumn[currentLeftVector.length + currentRightVector.length];
			index = 0;
			for(DBColumn dbcolumn: currentLeftVector){
				Object[] block = null;
				switch(dbcolumn.getDataType()){
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
				for (int i=0;i<leftMatchingEntries.size();i++){
					blockResult[i] = block[leftMatchingEntries.get(i)];
				}
				joinResult[index++] = new DBColumn(blockResult, dbcolumn.getDataType());
			}
			for(DBColumn dbcolumn: currentRightVector){
				Object[] block = null;
				switch(dbcolumn.getDataType()){
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
				for (int i=0;i<rightMatchingEntries.size();i++){
					blockResult[i] = block[rightMatchingEntries.get(i)];
				}
				joinResult[index++] = new DBColumn(blockResult, dbcolumn.getDataType());
			}
			return joinResult;
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		leftChild.close();
		rightChild.close();
	}
}
