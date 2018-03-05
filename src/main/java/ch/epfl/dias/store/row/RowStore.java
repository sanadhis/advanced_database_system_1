package ch.epfl.dias.store.row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

// Additional imported libraries
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class RowStore extends Store {

	// TODO: Add required structures
	public DataType[] schema;
	public String filename;
	public String delimiter;
	public ArrayList<DBTuple> relationTuple;

	public RowStore(DataType[] schema, String filename, String delimiter) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public void load() {
		// TODO: Implement
		Path pathToFile = Paths.get("./input/" + filename);
		try {
			InputStream in = Files.newInputStream(pathToFile);
			BufferedReader reader =	new BufferedReader(new InputStreamReader(in)); 
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] data = line.split(delimiter);
				DBTuple perTuple = new DBTuple(data,schema);
				relationTuple.add(perTuple);
			}
		} catch (IOException x) {
			System.err.println(x);
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		DBTuple result = relationTuple.get(rownumber);
		return result;
	}
}
