package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class ColumnarVectorTest {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;

    ColumnStore columnstoreData;
    ColumnStore columnstoreOrder;
    ColumnStore columnstoreLineItem;

    @Before
    public void init() {

        schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
                DataType.INT, DataType.INT, DataType.INT, DataType.INT };

        orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

        lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
                DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

        columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
        columnstoreData.load();

        columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
        columnstoreOrder.load();

        columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        columnstoreLineItem.load();
    }

    @Test
    public void spTestData1() {
        /* SELECT COUNT(*) FROM data WHERE col4 == 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, 4);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
        sel.open();
        DBColumn[] test = sel.next();
        while(test!=null){
            for(Object obj: test[0].getAsInteger()){
                System.out.println(obj);
            }
            test = sel.next();
        }

        int output = 3;

        assertTrue(output == 3);
    }

    @Test
    public void spTestData() {
        /* SELECT COUNT(*) FROM data WHERE col4 == 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, 4);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);

        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);
        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 3);
    }

    @Test
    public void spTestOrder() {
        /* SELECT COUNT(*) FROM data WHERE col0 == 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 5);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 6);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 1);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 1);
    }

    @Test
    public void spTestLineItem() {
        /* SELECT COUNT(*) FROM data WHERE col0 == 3 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 10);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT,
                DataType.INT, 2);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];

        assertTrue(output == 3);
    }

    @Test
    public void spTestLineItem2() {
        /* SELECT COUNT(*) FROM data WHERE col0 == 3 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 3);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.COUNT,
                DataType.INT, 8);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        // System.out.println(output);
        assertTrue(output == 10);
    }

    @Test
    public void joinTest1() {
        /*
         * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
         * WHERE orderkey = 3;
         */

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 10);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 5);

        /* Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0,
                3);

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selOrder, selLineitem, 0, 0);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join,
                Aggregate.COUNT, DataType.INT, 0);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        // int output = 2;

        // DBColumn[] test = join.next();
        // while (test != null) {
        //     for (Object obj : test[0].getAsInteger()) {
        //         System.out.println(obj);
        //     }
        //     test = join.next();
        // }

        assertTrue(output == 3);
    }

    @Test
    public void joinTest2() {
        /*
         * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
         * WHERE orderkey = 3;
         */

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 3);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 4);

        /* Filtering on both sides */
        ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem, BinaryOp.EQ, 0, 3);

        ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selLineitem, selOrder, 0, 0);
        // DBColumn[] x = join.next();
        // System.out.println(x.length);
        // for (Double val: x[4].getAsDouble()){
        // 	System.out.println(val);
        // }
        // for (String str: x[24].getAsString()){
        // 	System.out.println(str);
        // }
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT,
                DataType.INT, 0);

        agg.open();
        DBColumn[] result = agg.next();

        // This query should return only one result
        int output = result[0].getAsInteger()[0];
        System.out.println(output);
        assertTrue(output == 3);
    }
}