package ch.epfl.dias.store.row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class RowStore extends Store {

    private DataType[] schema;
    private String filename;
    private String delimiter;
    private ArrayList<DBTuple> relationTuple;

    public RowStore(DataType[] schema, String filename, String delimiter) {
        this.schema = schema;
        this.filename = filename;
        this.delimiter = delimiter;
        this.relationTuple = new ArrayList<DBTuple>();
    }

    @Override
    public void load() {
        String projectPath = System.getProperty("user.dir");
        Path pathToFile = Paths.get(projectPath + "/" + filename);
        try {
            InputStream in = Files.newInputStream(pathToFile);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = reader.readLine()) != null) {
                Object[] perTupleFields = parseLine(line, schema);
                DBTuple perTuple = new DBTuple(perTupleFields, schema);
                relationTuple.add(perTuple);
            }
        } catch (IOException x) {
            System.err.println(x);
        }
    }

    @Override
    public DBTuple getRow(int rownumber) {
        try {
            DBTuple result = relationTuple.get(rownumber);
            return result;
        } catch (IndexOutOfBoundsException e) {
            return new DBTuple();
        }
    }

    public Object[] parseLine(String line, DataType[] schema) {
        Object[] parsedData = new Object[schema.length];
        String[] fields = line.split(delimiter);
        for (int i = 0; i < fields.length; i++) {
            switch (schema[i]) {
            case INT:
                parsedData[i] = Integer.parseInt(fields[i]);
                break;
            case DOUBLE:
                parsedData[i] = Double.parseDouble(fields[i]);
                break;
            case STRING:
                parsedData[i] = fields[i];
                break;
            case BOOLEAN:
                parsedData[i] = Boolean.parseBoolean(fields[i]);
                break;
            }
        }
        return parsedData;
    }
}
