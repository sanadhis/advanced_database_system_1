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

public class PAXStore extends Store {

	// TODO: Add required structures

	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		// TODO: Implement
	}

	@Override
	public void load() {
		// TODO: Implement
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement
		return null;
	}
}
