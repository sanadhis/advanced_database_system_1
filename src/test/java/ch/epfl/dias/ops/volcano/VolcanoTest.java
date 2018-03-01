package ch.epfl.dias.ops.volcano;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import org.junit.Before;
import org.junit.Test;

public class VolcanoTest {

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
                DataType.DOUBLE,
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
	public void spTestData(){
	    /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 3, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}
	
	@Test
	public void spTestOrder(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 1);
	}
	
	@Test
	public void spTestLineItem(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 3);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}

	@Test
	public void joinTest1(){
	    /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selOrder,selLineitem,0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
	}
	
	@Test
	public void joinTest2(){
	    /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selLineitem,selOrder,0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
	}
}
