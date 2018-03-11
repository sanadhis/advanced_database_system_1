package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;

public class DBPAXpage {

	private ArrayList<DBColumn> PAXminipages;
	private boolean eof;
	// TODO: Implement

	public DBPAXpage(Object[][] pagesRecord, DataType[] types){
		PAXminipages = new ArrayList<DBColumn>();
		
		int index = 0;
		for(Object[] pageAttribute: pagesRecord){
			DBColumn minipage = new DBColumn(pageAttribute, types[index++]);
			PAXminipages.add(minipage);
		}

		this.eof = false;
	}

	public DBPAXpage(){
		this.eof = true;
	}

	public DBTuple getTuple(int rowNumber){
		Object[] composedTuple = new Object[PAXminipages.size()];
		DataType[] types = new DataType[PAXminipages.size()];
		int index = 0;
		for(DBColumn minipage: PAXminipages){
			types[index] = minipage.getDataType();
			switch(types[index]){
				case INT:
					composedTuple[index] = minipage.getAsInteger()[rowNumber];
					break;
				case DOUBLE:
					composedTuple[index] = minipage.getAsDouble()[rowNumber];
					break;
				case STRING:
					composedTuple[index] = minipage.getAsString()[rowNumber];
					break;
				case BOOLEAN:
					composedTuple[index] = minipage.getAsBoolean()[rowNumber];
					break;
			}
			index++;
		}
		return new DBTuple(composedTuple, types);
	}
}
