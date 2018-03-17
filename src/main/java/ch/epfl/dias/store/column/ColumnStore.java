package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.util.ArrayList;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.List;

public class ColumnStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;
	private ArrayList<DBColumn> relationColumn;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.relationColumn = new ArrayList<DBColumn>(schema.length);
	}

	@Override
	public void load() {
		String projectPath = System.getProperty("user.dir");
		Path pathToFile = Paths.get(projectPath + "/" + filename);
		try {
			// Init ArrayList to collect all data
			List<List<Object>> genericArray = new ArrayList<List<Object>>();
			for (int i = 0; i < schema.length; i++) {
				genericArray.add(new ArrayList<Object>());
			}

			// read per line
			InputStream in = Files.newInputStream(pathToFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] rawLineData = line.split(delimiter);
				for (int i = 0; i < rawLineData.length; i++) {
					Object perFieldData = parseData(rawLineData[i], schema[i]);
					genericArray.get(i).add(perFieldData);
				}
			}

			// convert ArrayList into Array
			int index = 0;
			for (List<Object> perColumnData : genericArray) {
				Object[] perColumnArray = new Object[perColumnData.size()];
				perColumnArray = perColumnData.toArray(perColumnArray);
				DBColumn perColumn = new DBColumn(perColumnArray, schema[index++]);
				relationColumn.add(perColumn);
			}
		} catch (IOException x) {
			System.err.println(x);
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		if (columnsToGet.length == 0) {
			columnsToGet = getAllColumns();
		}
		DBColumn[] selectedColumns = new DBColumn[columnsToGet.length];
		for (int i = 0; i < columnsToGet.length; i++) {
			selectedColumns[i] = relationColumn.get(columnsToGet[i]);
		}
		return selectedColumns;
	}

	public int getNumberOfColumns() {
		return schema.length;
	}

	public int[] getAllColumns() {
		int numberOfColumns = this.getNumberOfColumns();
		int[] allFieldsNo = new int[numberOfColumns];
		for (int i = 0; i < numberOfColumns; i++) {
			allFieldsNo[i] = i;
		}
		return allFieldsNo;
	}

	public Object parseData(String field, DataType fieldDataType) {
		Object parsedData = null;
		switch (fieldDataType) {
		case INT:
			parsedData = Integer.parseInt(field);
			break;
		case DOUBLE:
			parsedData = Double.parseDouble(field);
			break;
		case STRING:
			parsedData = field;
			break;
		case BOOLEAN:
			parsedData = Boolean.parseBoolean(field);
			break;
		}
		return parsedData;
	}
}
