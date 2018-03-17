package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class HashJoin implements VolcanoOperator {

	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private Hashtable<Integer, ArrayList<DBTuple>> htable;
	private Iterator<DBTuple> it;
	private ArrayList<DBTuple> matchingTuple;
	private DBTuple currentTuple;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.htable = new Hashtable<Integer, ArrayList<DBTuple>>();
		this.matchingTuple = new ArrayList<DBTuple>();
	}

	@Override
	public void open() {
		leftChild.open();
		rightChild.open();
		DBTuple currentLeftTuple = leftChild.next();
		while (!currentLeftTuple.eof) {
			buildHashTable(currentLeftTuple, leftFieldNo);
			currentLeftTuple = leftChild.next();
		}
		currentTuple = rightChild.next();
		Integer currentValue = currentTuple.getFieldAsInt(rightFieldNo);
		getHashKeysByValue(currentValue);
	}

	@Override
	public DBTuple next() {
		if (currentTuple.eof) {
			return currentTuple;
		} else {
			if (it.hasNext()) {
				DBTuple leftSection = it.next();
				return leftSection;
			} else {
				currentTuple = rightChild.next();
				if (currentTuple.eof) {
					return currentTuple;
				} else {
					Integer currentValue = currentTuple.getFieldAsInt(rightFieldNo);
					getHashKeysByValue(currentValue);
					return this.next();
				}
			}
		}
	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}

	public void buildHashTable(DBTuple currentTuple, int fieldNo) {
		Integer fieldValue = currentTuple.getFieldAsInt(fieldNo);
		try {
			htable.get(fieldValue).add(currentTuple);
		} catch (NullPointerException e) {
			htable.put(fieldValue, new ArrayList<DBTuple>());
			htable.get(fieldValue).add(currentTuple);
		}
	}

	public void getHashKeysByValue(Integer value) {
		matchingTuple = htable.get(value);
		it = matchingTuple.iterator();
	}

}
