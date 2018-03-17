package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;

public class PAXStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;
	private int tuplesPerPage;
	private ArrayList<DBPAXpage> PAXpages;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
		this.PAXpages = new ArrayList<DBPAXpage>();
	}

	@Override
	public void load() {
		String projectPath = System.getProperty("user.dir");
		Path pathToFile = Paths.get(projectPath + "/" + filename);

		try {
			Object[][] perPageRecords = new Object[schema.length][tuplesPerPage];

			InputStream in = Files.newInputStream(pathToFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line = null;

			int tupleCounter = 0;
			while ((line = reader.readLine()) != null) {
				String[] rawLineData = line.split(delimiter);
				for (int i = 0; i < rawLineData.length; i++) {
					Object perFieldData = parseData(rawLineData[i], schema[i]);
					perPageRecords[i][tupleCounter] = perFieldData;
				}
				tupleCounter++;
				if (tupleCounter == tuplesPerPage) {
					PAXpages.add(new DBPAXpage(perPageRecords, schema));
					perPageRecords = new Object[schema.length][tuplesPerPage];
					tupleCounter = 0;
				}
			}
			if (tupleCounter != 0) {
				PAXpages.add(new DBPAXpage(perPageRecords, schema));
			}
		} catch (IOException x) {
			System.err.println(x);
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		int pageNumber = rownumber / tuplesPerPage;
		int relativeRowNumber = rownumber % tuplesPerPage;
		try {
			DBTuple rowResult = PAXpages.get(pageNumber).getTuple(relativeRowNumber);
			return rowResult;
		} catch (IndexOutOfBoundsException e) {
			return new DBTuple();
		}
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
