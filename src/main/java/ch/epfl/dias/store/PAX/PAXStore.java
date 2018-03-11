package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import java.util.List;


public class PAXStore extends Store {

	// TODO: Add required structures
	private DataType[] schema;
	private String filename;
	private String delimiter;
	private int tuplesPerPage;
	private ArrayList<DBPAXpage> paxPages;

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
		this.paxPages = new ArrayList<DBPAXpage>();
	}

	@Override
	public void load() {
		// TODO: Implement
		String projectPath = System.getProperty("user.dir");
		Path pathToFile = Paths.get(projectPath + "/" +filename);

		try {
			InputStream in = Files.newInputStream(pathToFile);
			BufferedReader reader =	new BufferedReader(new InputStreamReader(in)); 
			
			Object[][] genericArr = new Object[schema.length][tuplesPerPage];
			int counter = 0;
			String line = null;

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
					genericArr[i][counter] = datapoint;
				}
				counter++;
				if (counter == tuplesPerPage){
					paxPages.add(new DBPAXpage(genericArr, schema));
					genericArr = new Object[schema.length][tuplesPerPage];
					counter = 0;
				}
			}
			if (counter != 0){
				paxPages.add(new DBPAXpage(genericArr, schema));
			}
		} catch (IOException x) {
			System.err.println(x);
		}
	}

	@Override
	public DBTuple getRow(int rownumber) {
		int pagePosition = rownumber / tuplesPerPage ;
		int rowPosition = rownumber % tuplesPerPage ;
		try{
			DBTuple rowResult = paxPages.get(pagePosition).getTuple(rowPosition);
			return rowResult;
		}
		catch(IndexOutOfBoundsException e){
			return new DBTuple();
		}
	}
}
