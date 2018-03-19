package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.block.BlockOperator;
import ch.epfl.dias.store.column.DBColumn;

public class Project implements BlockOperator {

    private BlockOperator childOperator;
    private int[] columns;

    public Project(BlockOperator child, int[] columns) {
        this.childOperator = child;
        this.columns = columns;
    }

    public DBColumn[] execute() {
        DBColumn[] childBlock = childOperator.execute();
        DBColumn[] results = new DBColumn[columns.length];
        int index = 0;
        for (int columnToGet : columns) {
            results[index++] = childBlock[columnToGet];
        }
        return results;
    }
}
