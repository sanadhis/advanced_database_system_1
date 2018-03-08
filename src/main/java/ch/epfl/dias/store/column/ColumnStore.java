package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.List;

public class ColumnStore extends Store {

	// TODO: Add required structures
	private DataType[] schema;
	private String filename;
	private String delimiter;
	private ArrayList<DBColumn> relationColumn;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.relationColumn = new ArrayList<DBColumn>(schema.length);
	}

	public int getNumberOfColumns(){
		return schema.length();
	}

	@Override
	public void load() {
		// TODO: Implement
		String projectPath = System.getProperty("user.dir");
		Path pathToFile = Paths.get(projectPath + "/" +filename);
		try {
			InputStream in = Files.newInputStream(pathToFile);
			BufferedReader reader =	new BufferedReader(new InputStreamReader(in)); 
			String line = null;
			List<List<Object>> genericArr = new ArrayList<List<Object>>();
			
			for (int i=0; i<schema.length; i++){
				genericArr.add(new ArrayList<Object>());
			}

			while ((line = reader.readLine()) != null) {
				String[] data = line.split(delimiter);
				for (int i=0; i<data.length; i++){
					Object datapoint = null;
					switch(schema[i]){
						case INT:
							datapoint = Integer.parseInt(data[i]);
							break;
						case DOUBLE:
							datapoint = Double.parseDouble(data[i]);
							break;
						case STRING:
							datapoint = data[i];
							break;
						case BOOLEAN:
							datapoint = Boolean.parseBoolean(data[i]);
							break;
					}
					genericArr.get(i).add(datapoint);
				}
			}
			int index = 0;
			for (List<Object> arr: genericArr){
				Object[] fields = new Object[arr.size()];
				fields = arr.toArray(fields);
				DBColumn perColumn = new DBColumn(fields,schema[index++]);
				relationColumn.add(perColumn);
			}
		} catch (IOException x) {
			System.err.println(x);
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		DBColumn[] columnsResult = new DBColumn[columnsToGet.length];
		for(int i=0; i<columnsToGet.length; i++){
			columnsResult[i] = relationColumn.get(columnsToGet[i]);
		}
		return columnsResult;
	}
}
