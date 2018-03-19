package ch.epfl.dias.ops.task3;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.*;

import org.junit.Before;
import org.junit.Test;

public class ColumnarBck {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;
    
    ColumnStore ColumnStoreData;
    ColumnStore ColumnStoreOrder;
    ColumnStore ColumnStoreLineItem;
    
    @Before
    public void init()  {
        
        schema = new DataType[]{ 
                DataType.INT, 
                DataType.INT, 
                DataType.INT, 
                DataType.INT, 
                DataType.INT,
                DataType.INT, 
                DataType.INT, 
                DataType.INT, 
                DataType.INT, 
                DataType.INT };
        
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
        
        ColumnStoreData = new ColumnStore(schema, "input/data.csv", ",");
        ColumnStoreData.load();
        
        ColumnStoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
        ColumnStoreOrder.load();
        
        ColumnStoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        ColumnStoreLineItem.load();        
    }
    
    @Test
    public void query1(){
        /* SELECT L_PARTKEY, L_DISCOUNT, L_SHIPDATE, L_SHIPMODE FROM LINEITEM WHERE L_LINENUMBER!=1 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.NE, 3, 1);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(sel, new int[]{1,6,10,14});
            
        // This query should return only one result
        DBColumn[] result = proj.execute();
        int output = result[0].getAsInteger()[result[0].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 1284483);
    }
    
    @Test
    public void query2(){
        /* SELECT L_SUPPKEY, L_TAX, L_RECEIPTDATE, L_COMMENT FROM LINEITEM WHERE L_PARTKEY<50000 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.LE, 1, 50000);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(sel, new int[]{2,7,11,15});
            
        // This query should return only one result
        DBColumn[] result = proj.execute();
        int output = result[0].getAsInteger()[result[0].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 17971);
    }
    
    @Test
    public void query3(){
        /* SELECT L_SUPPKEY, L_RETURNFLAG, L_LINESTATUS FROM LINEITEM WHERE L_SUPPKEY>34508 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(scan, new int[]{2,8,9});
        ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(proj, BinaryOp.GE, 0, 34508);
            
        // This query should return only one result
        DBColumn[] result = sel.execute();
        int output = result[0].getAsInteger()[result[0].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 34508);
    }

    @Test
    public void query4(){
        /* SELECT L_SUPPKEY, L_RETURNFLAG, L_LINESTATUS FROM LINEITEM WHERE L_SUPPKEY>34508 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(scan, new int[]{2,8,9});
        ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(proj, BinaryOp.GT, 0, 34508);
            
        // This query should return only one result
        DBColumn[] result = sel.execute();
        int output = result[0].getAsInteger()[result[0].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 65359);
    }

    @Test
    public void joinTest1(){
        /* SELECT * FROM order O JOIN lineitem ON (o_orderkey = orderkey);*/
    
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
        ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
    
        /*Filtering on both sides */
        // ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.EQ,0,3);
        // ch.epfl.dias.ops.block.Select selLineitem = new ch.epfl.dias.ops.block.Select(scanLineitem, BinaryOp.EQ,0,3);
    
        ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(scanOrder,scanLineitem,0,0);
        // ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);

        DBColumn[] result = join.execute();
        int output = result[13].getAsInteger()[result[13].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 27);
    }
    
    @Test
    public void joinTest2(){
        /* SELECT * FROM order O JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 780017;*/
    
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
        ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.GE, 1, 780017);
    
        ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(selOrder,scanLineitem,0,0);
        // ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);

        DBColumn[] result = join.execute();
        int output = result[13].getAsInteger()[result[13].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 27);
    }
    
    @Test
    public void joinTest3(){
        /* SELECT * FROM order O JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 780017;*/
    
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
        ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.GE, 1, 780017);
        ch.epfl.dias.ops.block.Project projLineItem = new ch.epfl.dias.ops.block.Project(scanLineitem, new int[]{0,1,3});

        ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(selOrder,projLineItem,0,0);
        // ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);

        DBColumn[] result = join.execute();
        int output = result[10].getAsInteger()[result[10].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 1284483);
    }
    
    @Test
    public void joinTest4(){
        /* SELECT L.L_COMMENT, O.O_SHIPPRIORITY FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 780017;*/
    
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
        ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.GE, 1, 780017);

        ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(scanLineitem ,selOrder,0,0);
        // ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);
    
        ch.epfl.dias.ops.block.Project projFinal = new ch.epfl.dias.ops.block.Project(join, new int[]{15,23});

        DBColumn[] result = projFinal.execute();
        int output = result[1].getAsInteger()[result[1].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 0);
    }
    
    @Test
    public void joinTest5(){
        // same as above but with to projection
        /* SELECT L.L_COMMENT, O.O_SHIPPRIORITY FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 780017;*/
    
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
        ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.GE, 1, 780017);
        ch.epfl.dias.ops.block.Project projOrder = new ch.epfl.dias.ops.block.Project(selOrder, new int[]{0,7});

        ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(scanLineitem ,projOrder,0,0);
        // ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);
    
        ch.epfl.dias.ops.block.Project projFinal = new ch.epfl.dias.ops.block.Project(join, new int[]{15,17});

        DBColumn[] result = projFinal.execute();
        int output = result[1].getAsInteger()[result[1].getAsInteger().length-1];
        System.out.println();
        assertTrue(output == 0);
    }
    
    // /*
    //     Testing Aggregation Start from here
    // */
    @Test
    public void query5(){
        /* SELECT MAX(L_DISCOUNT) FROM lineitem */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(scan, new int[]{6});
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(proj, Aggregate.MAX, DataType.DOUBLE, 0);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
        System.out.println(output + "\n");
        assertTrue(output == 0.1);
    }
    
    @Test
    public void query6(){
        /* SELECT MIN(L_DISCOUNT) FROM lineitem where L_QUANTITY>=30 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.GE, 4, 30);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(proj, Aggregate.MIN, DataType.DOUBLE, 0);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
        System.out.println(output + "\n");
        assertTrue(output == 0.0);
    }
    
    @Test
    public void query7(){
        /* SELECT MIN(L_DISCOUNT) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(proj, Aggregate.MIN, DataType.DOUBLE, 0);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
        System.out.println(output + "\n");
        assertTrue(output == 0.04);
    }
    
    @Test
    public void query8(){
        /* SELECT SUM(L_DISCOUNT) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(proj, Aggregate.SUM, DataType.DOUBLE, 0);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
        System.out.println(output + "\n");
        assertTrue(output == 0.39);
    }
    
    @Test
    public void query9(){
        /* SELECT AVG(L_DISCOUNT) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(proj, Aggregate.AVG, DataType.DOUBLE, 0);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
        System.out.println(output + "\n");
        assertTrue(output == 0.078);
    }
    
    @Test
    public void query10(){
        /* SELECT AVG(L_PARTKEY) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(selOrder, new int[]{0});
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(proj, Aggregate.AVG, DataType.DOUBLE, 0);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
        System.out.println(output + "\n");
        assertTrue(output == 1.4);
    }
    
    @Test
    public void query11(){
        /* SELECT SUM(O.O_CUSTKEY) FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 1000000;*/
    
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
        ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.GE, 1, 1000000);

        ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(scanLineitem ,selOrder,0,0);
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.SUM, DataType.INT, 17);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        int output = result[0].getAsInteger()[0];
        System.out.println(output + "\n");
        assertTrue(output == 3699420);
    }

    @Test
    public void query12(){
        /* SELECT AVG(O.O_CUSTKEY) FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 1000000;*/
    
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
        ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.GE, 1, 1000000);

        ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(selOrder ,scanLineitem,0,0);
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.AVG, DataType.DOUBLE, 1);
            
        // This query should return only one result
        DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
        System.out.println(output + "\n");
        assertTrue(output == 1233140.0);
    }
    
}
