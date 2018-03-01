package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	// TODO: Add required structures

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		// TODO: Implement
	}

	@Override
	public void load() {
		// TODO: Implement
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		// TODO: Implement
		return null;
	}
}
