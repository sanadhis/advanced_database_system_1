package ch.epfl.dias.ops.task3;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.Project;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import org.junit.Before;
import org.junit.Test;

public class RowVolcano {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;
    
    RowStore rowstoreData;
    RowStore rowstoreOrder;
    RowStore rowstoreLineItem;
    
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
        
        rowstoreData = new RowStore(schema, "input/data.csv", ",");
        rowstoreData.load();
        
        rowstoreOrder = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
        rowstoreOrder.load();
        
        rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        rowstoreLineItem.load();        
    }
    
    @Test
    public void query1(){
        /* SELECT L_PARTKEY, L_DISCOUNT, L_SHIPDATE, L_SHIPMODE FROM LINEITEM WHERE L_LINENUMBER!=1 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.NE, 3, 1);
        ch.epfl.dias.ops.volcano.Project proj = new Project(sel, new int[]{1,6,10,14});
    
        proj.open();
        
        // This query should return only one result
        DBTuple result = proj.next();
        int output = result.getFieldAsInt(0);
        while(!result.eof){
            System.out.println(result.getFieldAsString(3));
            output = result.getFieldAsInt(0);            
            result = proj.next();
        }
        System.out.println();
        assertTrue(output == 1284483);
    }
    
    @Test
    public void query2(){
        /* SELECT L_SUPPKEY, L_TAX, L_RECEIPTDATE, L_COMMENT FROM LINEITEM WHERE L_PARTKEY<50000 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LE, 1, 50000);
        ch.epfl.dias.ops.volcano.Project proj = new Project(sel, new int[]{2,7,11,15});
    
        proj.open();
        
        // This query should return only one result
        DBTuple result = proj.next();
        int output = result.getFieldAsInt(0);
        while(!result.eof){
            System.out.println(result.getFieldAsDouble(1));
            output = result.getFieldAsInt(0);            
            result = proj.next();
        }
        System.out.println();
        assertTrue(output == 17971);
    }
    
    @Test
    public void query3(){
        /* SELECT L_SUPPKEY, L_RETURNFLAG, L_LINESTATUS FROM LINEITEM WHERE L_SUPPKEY>34508 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Project proj = new Project(scan, new int[]{2,8,9});
        ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(proj, BinaryOp.GE, 0, 34508);
    
        sel.open();
        
        // This query should return only one result
        DBTuple result = sel.next();
        int output = result.getFieldAsInt(0);
        while(!result.eof){
            System.out.println(result.getFieldAsInt(0));
            output = result.getFieldAsInt(0);            
            result = sel.next();
        }
        System.out.println();
        assertTrue(output == 34508);
    }

    @Test
    public void query4(){
        /* SELECT L_SUPPKEY, L_RETURNFLAG, L_LINESTATUS FROM LINEITEM WHERE L_SUPPKEY>34508 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Project proj = new Project(scan, new int[]{2,8,9});
        ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(proj, BinaryOp.GT, 0, 34508);
    
        sel.open();
        
        // This query should return only one result
        DBTuple result = sel.next();
        int output = result.getFieldAsInt(0);
        while(!result.eof){
            System.out.println(result.getFieldAsInt(0));
            output = result.getFieldAsInt(0);            
            result = sel.next();
        }
        System.out.println();
        assertTrue(output == 65359);
    }

    @Test
    public void joinTest1(){
        /* SELECT * FROM order O JOIN lineitem ON (o_orderkey = orderkey);*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
    
        /*Filtering on both sides */
        // ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scanOrder, BinaryOp.EQ,0,3);
        // ch.epfl.dias.ops.volcano.Select selLineitem = new ch.epfl.dias.ops.volcano.Select(scanLineitem, BinaryOp.EQ,0,3);
    
        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scanOrder,scanLineitem,0,0);
        // ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);
    
        join.open();

        DBTuple result = join.next();
        int output = result.getFieldAsInt(13);
        while(!result.eof){
            System.out.println(result.getFieldAsString(4));
            output = result.getFieldAsInt(13);   
            System.out.println(output);         
            result = join.next();
        }
        System.out.println();
        assertTrue(output == 27);
    }
    
    @Test
    public void joinTest2(){
        /* SELECT * FROM order O JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 780017;*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scanOrder, BinaryOp.GE, 1, 780017);
    
        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(selOrder,scanLineitem,0,0);
        // ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);
    
        join.open();

        DBTuple result = join.next();
        int output = result.getFieldAsInt(13);
        while(!result.eof){
            System.out.println(result.getFieldAsString(4));
            output = result.getFieldAsInt(13);   
            System.out.println(output);         
            result = join.next();
        }
        System.out.println();
        assertTrue(output == 27);
    }
    
    @Test
    public void joinTest3(){
        /* SELECT * FROM order O JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 780017;*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scanOrder, BinaryOp.GE, 1, 780017);
        ch.epfl.dias.ops.volcano.Project projLineItem = new Project(scanLineitem, new int[]{0,1,3});

        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(selOrder,projLineItem,0,0);
        // ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);
    
        join.open();

        DBTuple result = join.next();
        int output = result.getFieldAsInt(10);
        while(!result.eof){
            System.out.println(result.getFieldAsString(4));
            output = result.getFieldAsInt(10);   
            System.out.println(output + " " +result.getFieldAsInt(11));         
            result = join.next();
        }
        System.out.println();
        assertTrue(output == 1284483);
    }
    
    @Test
    public void joinTest4(){
        /* SELECT L.L_COMMENT, O.O_SHIPPRIORITY FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 780017;*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scanOrder, BinaryOp.GE, 1, 780017);

        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scanLineitem ,selOrder,0,0);
        // ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);
    
        ch.epfl.dias.ops.volcano.Project projFinal = new Project(join, new int[]{15,23});
        projFinal.open();

        DBTuple result = projFinal.next();
        int output = result.getFieldAsInt(1);
        while(!result.eof){
            System.out.println(result.getFieldAsString(0)+  " " +result.getFieldAsInt(1));         
            output = result.getFieldAsInt(1);   
            result = projFinal.next();
        }
        System.out.println();
        assertTrue(output == 0);
    }
    
    @Test
    public void joinTest5(){
        // same as above but with to projection
        /* SELECT L.L_COMMENT, O.O_SHIPPRIORITY FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 780017;*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scanOrder, BinaryOp.GE, 1, 780017);
        ch.epfl.dias.ops.volcano.Project projOrder = new Project(selOrder, new int[]{0,7});

        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scanLineitem ,projOrder,0,0);
        // ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join,Aggregate.COUNT, DataType.INT, 0);
    
        ch.epfl.dias.ops.volcano.Project projFinal = new Project(join, new int[]{15,17});
        projFinal.open();

        DBTuple result = projFinal.next();
        int output = result.getFieldAsInt(1);
        while(!result.eof){
            System.out.println(result.getFieldAsString(0)+  " " +result.getFieldAsInt(1));         
            output = result.getFieldAsInt(1);   
            result = projFinal.next();
        }
        System.out.println();
        assertTrue(output == 0);
    }
    
    /*
        Testing Aggregation Start from here
    */
    @Test
    public void query5(){
        /* SELECT MAX(L_DISCOUNT) FROM lineitem */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Project proj = new Project(scan, new int[]{6});
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(proj, Aggregate.MAX, DataType.DOUBLE, 0);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        System.out.println(output + "\n");

        assertTrue(output == 0.1);
    }
    
    @Test
    public void query6(){
        /* SELECT MIN(L_DISCOUNT) FROM lineitem where L_QUANTITY>=30 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 4, 30);
        ch.epfl.dias.ops.volcano.Project proj = new Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(proj, Aggregate.MIN, DataType.DOUBLE, 0);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        System.out.println(output + "\n");

        assertTrue(output == 0.0);
    }
    
    @Test
    public void query7(){
        /* SELECT MIN(L_DISCOUNT) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.volcano.Project proj = new Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(proj, Aggregate.MIN, DataType.DOUBLE, 0);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        System.out.println(output + "\n");

        assertTrue(output == 0.04);
    }
    
    @Test
    public void query8(){
        /* SELECT SUM(L_DISCOUNT) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.volcano.Project proj = new Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(proj, Aggregate.SUM, DataType.DOUBLE, 0);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        System.out.println(output + "\n");

        assertTrue(output == 0.39);
    }
    
    @Test
    public void query9(){
        /* SELECT AVG(L_DISCOUNT) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.volcano.Project proj = new Project(selOrder, new int[]{6});
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(proj, Aggregate.AVG, DataType.DOUBLE, 0);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        System.out.println(output + "\n");

        assertTrue(output == 0.078);
    }
    
    @Test
    public void query10(){
        /* SELECT AVG(L_PARTKEY) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.LT, 4, 30);
        ch.epfl.dias.ops.volcano.Project proj = new Project(selOrder, new int[]{0});
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(proj, Aggregate.AVG, DataType.DOUBLE, 0);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        System.out.println(output + "\n");

        assertTrue(output == 1.4);
    }
    
    @Test
    public void query11(){
        /* SELECT SUM(O.O_CUSTKEY) FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 1000000;*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scanOrder, BinaryOp.GE, 1, 1000000);

        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scanLineitem ,selOrder,0,0);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join, Aggregate.SUM, DataType.INT, 17);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        int output = result.getFieldAsInt(0);
        System.out.println(output + "\n");

        assertTrue(output == 3699420);
    }

    @Test
    public void query12(){
        /* SELECT AVG(O.O_CUSTKEY) FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 1000000;*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
    
        /*Filtering on both sides */
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(scanOrder, BinaryOp.GE, 1, 1000000);

        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(selOrder ,scanLineitem,0,0);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join, Aggregate.AVG, DataType.DOUBLE, 1);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        Double output = result.getFieldAsDouble(0);
        System.out.println(output + "\n");

        assertTrue(output == 1233140.0);
    }
    
}
