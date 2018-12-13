package cn.itcast.bigdata.hbase;


import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.junit.Before;
import org.junit.Test;



public class HbaseTest {

	/*
	 * 配置ss
	 */
	private Configuration config = null;
	private HTable table = null;
	private HConnection connection = null;
	@Before
	public void init() throws Exception {

		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "server06:2181,server07:2181,server08:2181");
		
		
	}
	/*
	 * 创建一张表
	 */
	@Test
	public void createTable() throws Exception {
		
		//创建表管理类
		HBaseAdmin admin = new HBaseAdmin(config);
		//创建表描述类
		TableName tableName = TableName.valueOf("test01");
		HTableDescriptor desc = new HTableDescriptor(tableName);
		
		//创建列族描述类
		HColumnDescriptor family = new HColumnDescriptor("info");//列族
		//将列族添加到列表中
		desc.addFamily(family);
		
		HColumnDescriptor famiy2 = new HColumnDescriptor("info2");//列族
		//将列族添加到列表中
		desc.addFamily(famiy2);
		
		//创建表
		admin.createTable(desc);
		

	}
	@Test
	public void deleteTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(config);
		admin.disableTable("test03");
		admin.deleteTable("test03");
		admin.close();
		
		
	}
	@Test
	public void insertData() throws Exception {

		
		HTable table = new HTable(config, "test01");
		Put put = new Put(Bytes.toBytes("furong"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(23));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(0));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("tokio"));
		table.put(put);
		table.close();
		
		
	}
	
	/**
	 * 单条查询
	 * 
	 * @throws Exception
	 */
	@Test
	public void queryData() throws Exception {
		HTable table = new HTable(config, "test01");
		Get get = new Get(Bytes.toBytes("furong"));
		Result result = table.get(get);
		//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("address"))));
	}
	
	/**
	 * 全表扫描
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanData() throws Exception {
		HTable table = new HTable(config, "test01");

		Scan scan = new Scan();
		//scan.addFamily(Bytes.toBytes("info"));
		//scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
		//scan.setStartRow(Bytes.toBytes("wangsf_0"));
		scan.setStopRow(Bytes.toBytes("wangwu"));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("address"))));
			//System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
			//System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
		}
	}
	
}






