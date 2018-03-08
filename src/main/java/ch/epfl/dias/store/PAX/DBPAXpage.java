package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;

public class DBPAXpage {

	public ArrayList<DBColumn> PAXminipages;
	public DataType[] types;
	public boolean eof;
	// TODO: Implement

	public DBPAXpage(Object[][] pagesRecord, DataType[] types){
		PAXminipages = new ArrayList<DBColumn>();
		
		this.types = types;

		int index = 0;
		for(Object[] pageAttribute: pagesRecord){
			DBColumn minipage = new DBColumn(pageAttribute, this.types[index++]);
			PAXminipages.add(minipage);
		}

		this.eof = true;
	}

	public DBPAXpage(){
		this.eof = false;
	}

	public DBTuple getTuple(int rowNumber){
		Object[] composedTuple = new Object[types.length];
		int index = 0;
		for(DBColumn minipage: PAXminipages){
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
