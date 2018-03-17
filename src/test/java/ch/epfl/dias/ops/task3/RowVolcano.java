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
	    /* SELECT * FROM order O JOIN lineitem ON (o_orderkey = orderkey);*/
	
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
	
}
