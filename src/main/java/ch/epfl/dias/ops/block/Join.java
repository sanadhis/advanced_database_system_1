package ch.epfl.dias.ops.block;

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

public class Join implements BlockOperator {

	// TODO: Add required structures
	private BlockOperator leftChild;
	private BlockOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private Hashtable<Integer, ArrayList<Integer>> leftHtable;
	private Hashtable<Integer, ArrayList<Integer>> rightHtable;
	private ArrayList<Integer> leftMatchingEntries;
	private ArrayList<Integer> rightMatchingEntries;

	public Join(BlockOperator leftChild, BlockOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.leftHtable = new Hashtable<Integer, ArrayList<Integer>>();
		this.rightHtable = new Hashtable<Integer, ArrayList<Integer>>();
		this.leftMatchingEntries = new ArrayList<Integer>();
		this.rightMatchingEntries = new ArrayList<Integer>();

	}

	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] leftChildBlock = leftChild.execute();
		DBColumn[] rightChildBlock = rightChild.execute();
		
		int index = 0;
		for (Integer val: leftChildBlock[leftFieldNo].getAsInteger()){
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
		for (Integer val: rightChildBlock[rightFieldNo].getAsInteger()){
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
		for (Integer val: leftChildBlock[leftFieldNo].getAsInteger()){
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

		DBColumn[] joinResult = new DBColumn[leftChildBlock.length + rightChildBlock.length];
		index = 0;
		for(DBColumn dbcolumn: leftChildBlock){
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
		for(DBColumn dbcolumn: rightChildBlock){
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
