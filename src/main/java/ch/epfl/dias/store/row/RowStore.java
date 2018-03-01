package ch.epfl.dias.store.row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {

	// TODO: Add required structures

	public RowStore(DataType[] schema, String filename, String delimiter) {
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
