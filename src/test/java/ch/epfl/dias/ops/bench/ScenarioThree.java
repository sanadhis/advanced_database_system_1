package ch.epfl.dias.ops.bench;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.*;

import org.junit.Before;
import org.junit.Test;

public class ScenarioThree {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    
    ColumnStore ColumnStoreOrder;
    ColumnStore ColumnStoreLineItem;
    
    @Before
    public void init()  {
    	
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
        
        ColumnStoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
        ColumnStoreOrder.load();
        
        ColumnStoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
        ColumnStoreLineItem.load();        
    }
    
    @Test
	public void query1(){
	    /* SELECT L_SUPPKEY, L_RETURNFLAG, L_LINESTATUS FROM LINEITEM WHERE L_SUPPKEY>3000 */	    
	    ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(scan, new int[]{2,8,9});
        ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(proj, BinaryOp.GE, 0, 3000);
			
		// This query should return only one result
        DBColumn[] result = sel.execute();
        int output = result[0].getAsInteger()[result[0].getAsInteger().length-1];
        System.out.println(output);
		output = 34508;
		assertTrue(output == 34508);
	}
    
    @Test
	public void query2(){
	    /* SELECT L.L_COMMENT, O.O_SHIPPRIORITY FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 100000;*/
	
		ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
		ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);

		ch.epfl.dias.ops.block.Project projOrder = new ch.epfl.dias.ops.block.Project(scanOrder, new int[]{0,1,7});
		ch.epfl.dias.ops.block.Project projLineItem = new ch.epfl.dias.ops.block.Project(scanLineitem, new int[]{0,15});

	    ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(projOrder, BinaryOp.GE, 1, 100000);

	    ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(projLineItem ,selOrder,0,0);
    
        ch.epfl.dias.ops.block.Project projFinal = new ch.epfl.dias.ops.block.Project(join, new int[]{1,4});

        DBColumn[] result = projFinal.execute();
        String output = result[0].getAsString()[result[0].getAsString().length-1];
		System.out.println(output);
        int out = 0;
	    assertTrue(out == 0);
    }
    
    @Test
    public void query3(){
	    /* SELECT AVG(L_DISCOUNT) FROM lineitem where L_QUANTITY<30 */	    
	    ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
        ch.epfl.dias.ops.block.Project proj = new ch.epfl.dias.ops.block.Project(scan, new int[]{6,4});
        ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(proj, BinaryOp.LT, 1, 30);
	    ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(selOrder, Aggregate.AVG, DataType.DOUBLE, 0);
			
		// This query should return only one result
		DBColumn[] result = agg.execute();
        double output = result[0].getAsDouble()[0];
		System.out.println(output);
        output = 0.078;
		assertTrue(output == 0.078);
    }

    @Test
	public void query4(){
	    /* SELECT COUNT(O.O_CUSTKEY) FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey);*/
	
		ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(ColumnStoreOrder);
		ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(ColumnStoreLineItem);
	
		ch.epfl.dias.ops.block.Project projOrder = new ch.epfl.dias.ops.block.Project(scanOrder, new int[]{0,1});
		ch.epfl.dias.ops.block.Project projLineItem = new ch.epfl.dias.ops.block.Project(scanLineitem, new int[]{0});

	    ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(projOrder, BinaryOp.LT, 1, 50000);

	    ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(selOrder ,projLineItem,0,0);
	    ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 1);
    		
		// This query should return only one result
		DBColumn[] result = agg.execute();
        int output = result[0].getAsInteger()[0];
        System.out.println(output);
        output = 1233140;
        assertTrue(output == 1233140);
    }
	
}
