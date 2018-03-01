package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class ColumnarTest {

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
	public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
		ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreData);
		ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
		ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
		ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 0, 6);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 1);
	}

	@Test
	public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
		ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void joinTest1() {
		/*
		 * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
		ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.block.Select selLineitem = new ch.epfl.dias.ops.block.Select(scanLineitem, BinaryOp.EQ, 0, 3);

		ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(selOrder, selLineitem, 0, 0);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void joinTest2() {
		/*
		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
		ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.block.Select selLineitem = new ch.epfl.dias.ops.block.Select(scanLineitem, BinaryOp.EQ, 0, 3);

		ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(selLineitem, selOrder, 0, 0);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}
}