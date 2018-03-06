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
	public DataType[] schema;
	public String filename;
	public String delimiter;
	public ArrayList<DBColumn> relationColumn;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.relationColumn = new ArrayList<DBColumn>(schema.length);
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
			List<List<String>> genericArr =  new ArrayList<List<String>>();
			while ((line = reader.readLine()) != null) {
				String[] data = line.split(delimiter);
				for (int i=0; i<data.length; i++){
					genericArr.get(i).add(data[i]);
				}
			}
			for (List<String> arr: genericArr){
				String[] fields = new String[arr.size()];
				fields = arr.toArray(fields);
				DBColumn perColumn = new DBColumn(fields,schema[0]);
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
