package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;

public class DBPAXpage {

    private ArrayList<DBColumn> PAXminipages;

    public DBPAXpage(Object[][] page, DataType[] tupleDataType) {
        PAXminipages = new ArrayList<DBColumn>();

        int index = 0;
        for (Object[] miniPage : page) {
            DBColumn minipage = new DBColumn(miniPage, tupleDataType[index++]);
            PAXminipages.add(minipage);
        }
    }

    public DBTuple getTuple(int rowNumber) {
        Object[] tupleFields = new Object[PAXminipages.size()];
        DataType[] tupleDataType = new DataType[PAXminipages.size()];
        int index = 0;
        for (DBColumn minipage : PAXminipages) {
            tupleDataType[index] = minipage.getDataType();
            switch (tupleDataType[index]) {
            case INT:
                tupleFields[index] = minipage.getAsInteger()[rowNumber];
                break;
            case DOUBLE:
                tupleFields[index] = minipage.getAsDouble()[rowNumber];
                break;
            case STRING:
                tupleFields[index] = minipage.getAsString()[rowNumber];
                break;
            case BOOLEAN:
                tupleFields[index] = minipage.getAsBoolean()[rowNumber];
                break;
            }
            index++;
        }
        return new DBTuple(tupleFields, tupleDataType);
    }
}
