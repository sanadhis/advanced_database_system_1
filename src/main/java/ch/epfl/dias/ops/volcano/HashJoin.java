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
		while(!currentLeftTuple.eof){
			Integer val = currentLeftTuple.getFieldAsInt(leftFieldNo);
			try{
				htable.get(val).add(currentLeftTuple);
			}
			catch(NullPointerException e){
				htable.put(val, new ArrayList<DBTuple>());
				htable.get(val).add(currentLeftTuple);
			}
			currentLeftTuple = leftChild.next();
		}
		currentTuple = rightChild.next();
		Integer currentValue = currentTuple.getFieldAsInt(rightFieldNo);
		getKeysByValue(currentValue);
	}

	@Override
	public DBTuple next() {
		if(currentTuple.eof){
			return currentTuple;
		}
		else{
			if(it.hasNext()){
				DBTuple leftSection = it.next();
				return leftSection;
			}
			else{
				currentTuple = rightChild.next();
				if(currentTuple.eof){
					return currentTuple;
				}
				else{
					Integer currentValue = currentTuple.getFieldAsInt(rightFieldNo);
					getKeysByValue(currentValue);
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

	public void getKeysByValue(Integer value){
		matchingTuple = htable.get(value);
		it = matchingTuple.iterator();
	}

}
