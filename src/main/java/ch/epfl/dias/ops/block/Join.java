package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements BlockOperator {

	// TODO: Add required structures

	public Join(BlockOperator leftChild, BlockOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
	}

	public DBColumn[] execute() {
		// TODO: Implement
		return null;
	}
}
