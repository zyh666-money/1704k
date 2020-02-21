package com.bawei.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class BooksDaoTest {
	//创建连接对象
	private static Configuration conf;
	static{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.64.196");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
	@Test
	public void scanTest() throws IOException{
		//获取hbase对象
		Connection conn = getConnection();
		//获取表
		Table table = conn.getTable(TableName.valueOf("hbase_books"));
		Scan scan = new Scan();
		RowFilter filter = new RowFilter(CompareOp.LESS, new BinaryComparator(Bytes.toBytes("1000000000")));
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] row = result.getRow();
			byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("Book_Title"));
			System.out.println("ISBN："+Bytes.toString(row)+"，"+"书名："+Bytes.toString(value));
		}
		System.out.println("--------------------");
	}
	
	@Test
	public void yearBetweenTest() throws IOException{
		//获取hbase对象
		Connection conn = getConnection();
		//获取表
		Table table = conn.getTable(TableName.valueOf("hbase_books"));
		Scan scan = new Scan();
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("1990"));
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes("2000"));
		FilterList list = new FilterList(filter1,filter2);
		scan.setFilter(list);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			byte[] row = result.getRow();
			byte[] shum = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("Book_Title"));
			byte[] sbs = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("Publisher"));
			System.out.println("ISBN："+Bytes.toString(row));
			System.out.println("书名："+Bytes.toString(shum));
			System.out.println("出版商："+Bytes.toString(sbs));
		}
		System.out.println("--------------------");
		conn.close();
	}
	
	@Test
	public void countBooksByAuthor() throws IOException{
		int sum = 0;
		//获取hbase对象
		Connection conn = getConnection();
		//获取表
		Table table = conn.getTable(TableName.valueOf("hbase_books"));
		Scan scan = new Scan();
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("1990"));
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Year_Of_Publication"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes("1999"));
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("Book_Author"), CompareOp.EQUAL, Bytes.toBytes("Kathleen Duey"));
		FilterList list = new FilterList(filter1,filter2,filter3);
		scan.setFilter(list);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			sum++;
			byte[] shum = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("Book_Title"));
			System.out.println("图书："+Bytes.toString(shum));
		}
		System.out.println("图书总数："+sum);
		conn.close();
	}
	
	private static Connection getConnection() throws IOException{
		return ConnectionFactory.createConnection(conf);
	}
}
