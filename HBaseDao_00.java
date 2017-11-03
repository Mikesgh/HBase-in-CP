package com.cp.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseDao {

	 private static Configuration conf = null;
	 private static Connection conn = null;
	 private static HBaseAdmin admin = null;
	
	/**
	 * 读取HBase配置
	 * 创建HBase数据库连接实例conn
	 * 获取HBase数据库管理员操作对象admin 
	 */
	public static void init() throws Exception{

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181"); //zookeeper集群节点
		
		conn = ConnectionFactory.createConnection(conf);
		admin = (HBaseAdmin)conn.getAdmin();
	}
	
	/**
	 * 关闭数据库连接 
	 */
	public static void end() throws IOException{
		
		if(null!=admin){		//若admin数据库管理操作对象未关闭，则关闭
			admin.close();
		}
		if(null!=conn){
			conn.close();
		}
	}
	
	/**  DDL CREATE 创建表
	 *  创建表（HBase创建表需至少指定一个列族columnFamily） 
	 * @param tableName	表名
	 * @param columnFamilies	列族名数组，可指定至少1个
	 */
	public static void createTable(String tableName, String[] columnFamilies) throws MasterNotRunningException, ZooKeeperConnectionException, Exception{ 

		if(admin.tableExists(tableName)){	//判断表是否存在，是则不创建
			System.out.println(tableName+"表已存在！");
			System.exit(0);
		}else{
			//描述要创建的表
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));	
			
			for(String columnFamily : columnFamilies){		
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));	
			}
			
			admin.createTable(tableDesc);
			System.out.println(tableName+"表创建成功！");
		}
	}
	
	/**  DDL ALTER 修改表
	 *  修改表，增加一个表列族
	 * @param tableName	表名
	 * @param columnFamily	列族
	 */
	public static void addColumnFamily(String tableName, String columnFamily) throws IOException{
		
		//判断管理员操作对象是否存在，不存在则不操作
		if(null==admin){
			System.out.println("数据库管理员对象不存在！");
		}else{
			
			HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));	//获取表操作句柄
			
			//描述待增加的表列族，并新增
			HTableDescriptor desc = new HTableDescriptor(table.getTableDescriptor());
			desc.addFamily(new HColumnDescriptor(Bytes.toBytes(columnFamily)));
			
			admin.disableTable(tableName);  
			admin.modifyTable(Bytes.toBytes(tableName), desc);  
			admin.enableTable(tableName);
			
			//关闭表操作句柄
			table.close();
		}
	}
	
	/**  DDL ALTER 修改表
	 * 	修改表，删除一个表列族
	 * @param tableName	表名
	 * @param columnFamily	列族
	 */
	public static void deleteColumnFamily(String tableName, String columnFamily) throws IOException{
		//判断管理员操作对象是否存在，不存在则不操作
		if(null==admin){
			System.out.println("数据库管理员对象不存在！");
		}else{
			admin.deleteColumn(tableName, columnFamily);
		}
	}

	/**  DDL DELETE 删除表
	 *  删除表
	 * @param tableName	表名
	 */
	public static void deleteTable(String tableName) throws IOException{
		
		//判断管理员操作对象是否存在，不存在则不操作
		if(null==admin){
			System.out.println("数据库管理员对象不存在！");
			
		}else if(admin.tableExists(tableName)){	//判断表是否存在，是则删除

			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("表" + tableName + "删除成功！");
			
		}
	}
	
	
	/** DML 增
	 * 	增添数据，增添数据至指定的“cell”中
	 * @param tableName	表名
	 * @param rowKey	行键
	 * @param columnFamily	列族
	 * @param column	列名
	 * @param value	添加的数据
	 */
	public static void addRow(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException{

		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));	//获取表的操作句柄
		
		//描述数据的增添
		Put put = new Put(Bytes.toBytes(rowKey));	
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
		
		//关闭表操作句柄
		table.close();
	}
	
	/**  DML 删
	 *  删除数据，删除某行记录
	 * @param tableName	表名
	 * @param rowKey	行键
	 */
	public static void deleteRow(String tableName, String rowKey) throws IOException{

		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));	//获取表的操作句柄
		
		//描述待删除的数据
		Delete delete =  new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		
		//关闭表操作句柄
		table.close();
	}
	
	/**  DML 删
	 *  删除数据，删除多行记录
	 * @param tableName	表名
	 * @param rowKeys	行键数组
	 */
	public static void deleteRows(String tableName, String[] rowKeys) throws IOException{

		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));	//获取表操作句柄
		
		//创建一个数据删除描述List，用于存放待删除的多行记录描述
		List<Delete> deleteList = new ArrayList<Delete>();
		for(String rowkey : rowKeys){
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			deleteList.add(delete);
		}
		table.delete(deleteList);
		
		//关闭表操作句柄
		table.close();
	}	
	
	/**	DML 查
	 *  查询数据，查询指定行的记录
	 * @param tableName	表名
	 * @param rowKey 行键
	 */
	public static void getRow(String tableName, String rowKey) throws IOException{
		
		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));	//获取表的操作句柄
		
		//描述待查询的数据行
		Get get = new Get(Bytes.toBytes(rowKey));
		
		//获取行记录，通过rawcell()方法获取cell，再通过CellUtil工具遍历cell中的属性值
		Result result = table.get(get);		
		for(Cell cell : result.rawCells()){	 
			System.out.println(
					"行键："+ Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
					"列族："+ Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
					"列："+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
					"值："+ Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
					"时间戳："+ cell.getTimestamp()
					);
		}
		//关闭表操作句柄
		table.close();
	}
	
	/** DML 查
	 *  查询数据，查询整个表的行记录
	 * @param tableName	表名
	 */
	public static void scanTable(String tableName) throws IOException{

		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));	//获取表的操作句柄
		
		Scan scan = new Scan();	//创建一个表扫描对象

		//获取扫描到的所有行记录，逐行记录逐个cell遍历
		ResultScanner results = table.getScanner(scan);
		for(Result result : results){				
			for(Cell cell : result.rawCells()){				
				System.out.println(								
						"行键：" + Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
						"列族：" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
						"列：" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
						"值："+ Bytes.toString(CellUtil.cloneValue(cell)) + "\t" +
						"时间戳：" + cell.getTimestamp()
						);
			}
		}
		//关闭扫描结果接收对象以及表操作句柄
		results.close();
		table.close();
	}
	
	/**  DML 改 
	 *  追加数据，追加数据至指定cell中
	 * @param tableName	表名
	 * @param rowKey	行键
	 * @param columnFamily	列族
	 * @param column	列
	 * @param value 值
	 */
	public static void appendData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException{

		HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));	//获取表操作句柄
		
		//描述待追加的数据
		Append append = new Append(Bytes.toBytes(rowKey));
		append.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		
		table.append(append);
		
		//关闭表操作句柄
		table.close();
	}
	
}
