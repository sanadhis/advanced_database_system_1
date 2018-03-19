package ch.epfl.dias.ops.bench;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.Project;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.*;
import ch.epfl.dias.store.row.DBTuple;

import org.junit.Before;
import org.junit.Test;

public class ScenarioTwo {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    
    PAXStore PAXStoreOrder;
    PAXStore PAXStoreLineItem;
    
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
        
        PAXStoreOrder = new PAXStore(orderSchema, "input/orders_big.csv", "\\|", 100);
        PAXStoreOrder.load();
        
        PAXStoreLineItem = new PAXStore(lineitemSchema, "input/lineitem_big.csv", "\\|", 200);
        PAXStoreLineItem.load();        
    }
    
    @Test
    public void query1(){
        /* SELECT L_SUPPKEY, L_RETURNFLAG, L_LINESTATUS FROM LINEITEM WHERE L_SUPPKEY>3000 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(PAXStoreLineItem);
        ch.epfl.dias.ops.volcano.Project proj = new Project(scan, new int[]{2,8,9});
        ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(proj, BinaryOp.GE, 0, 3000);
    
        sel.open();
        
        // This query should return only one result
        DBTuple result = sel.next();
        int output = result.getFieldAsInt(0);
        while(!result.eof){
            output = result.getFieldAsInt(0);            
            result = sel.next();
        }
        System.out.println(output);
        output = 34508;
        assertTrue(output == 34508);
    }
    
    @Test
    public void query2(){
        /* SELECT L.L_COMMENT, O.O_SHIPPRIORITY FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey) where O.O_CUSTKEY >= 100000;*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(PAXStoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(PAXStoreLineItem);
    
        // Projection
        ch.epfl.dias.ops.volcano.Project projOrder = new ch.epfl.dias.ops.volcano.Project(scanOrder, new int[]{0,1,7});
        ch.epfl.dias.ops.volcano.Project projLineItem = new ch.epfl.dias.ops.volcano.Project(scanLineitem, new int[]{0,15});
        

        /*Filtering on both sides */
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(projOrder, BinaryOp.GE, 1, 100000);

        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(projLineItem ,selOrder,0,0);
    
        ch.epfl.dias.ops.volcano.Project projFinal = new Project(join, new int[]{1,4});
        projFinal.open();

        DBTuple result = projFinal.next();
        String output = result.getFieldAsString(0);
        while(!result.eof){
            output = result.getFieldAsString(0);   
            result = projFinal.next();
        }
        System.out.println(output);
        int out = 0;
        assertTrue(out == 0);
    }
    
    @Test
    public void query3(){
        /* SELECT AVG(L_DISCOUNT) FROM lineitem where L_QUANTITY<30 */	    
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(PAXStoreLineItem);
        ch.epfl.dias.ops.volcano.Project proj = new Project(scan, new int[]{6,4});
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(proj, BinaryOp.LT, 1, 30);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(selOrder, Aggregate.AVG, DataType.DOUBLE, 0);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        System.out.println(output);
        output = 0.078;
        assertTrue(output == 0.078);
    }

    @Test
    public void query4(){
        /* SELECT COUNT(O.O_CUSTKEY) FROM order O, lineitem L JOIN lineitem ON (o_orderkey = orderkey);*/
    
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(PAXStoreOrder);
        ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(PAXStoreLineItem);
        
        // Projection
        ch.epfl.dias.ops.volcano.Project projOrder = new ch.epfl.dias.ops.volcano.Project(scanOrder, new int[]{0,1});
        ch.epfl.dias.ops.volcano.Project projLineItem = new ch.epfl.dias.ops.volcano.Project(scanLineitem, new int[]{0});
            
        ch.epfl.dias.ops.volcano.Select selOrder = new ch.epfl.dias.ops.volcano.Select(projOrder, BinaryOp.LT, 1, 50000);

        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(selOrder ,projLineItem,0,0);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 1);
    
        agg.open();
        
        // This query should return only one result
        DBTuple result = agg.next();
        Integer output = result.getFieldAsInt(0);
        System.out.println(output);
        output = 1233140;
        assertTrue(output == 1233140);
    }
    
}
