package com.cp.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class MyHDFSDao {
	
	private static Configuration conf = new Configuration();		//配置信息对象,配置HDFS相关信息，用于HDFS文件系统的客户端的实例化
	private static FileSystem fs = null;		//HDFS文件系统的客户端对象，通过它来操作HDFS文件系统
	
	/**
	 * 获取HDFS文件系统客户端 
	 */
	public static void init() throws IOException{
		
		//设置HDFS文件系统相关配置参数
		conf.set("fs.defaultFS", "hdfs://master:9000/");
		conf.set("df.rep","2");
		fs = FileSystem.get(conf);		//根据配置文件的配置信息，实例化对应类型的文件系统客户端
	}
	
	/**
	 * put增   上传文件 至 HDFS
	 * @param localFilePath	待上传的文件路径
	 * @param destFilePath	上传至HDFS的文件路径
	 */
	public static void uploadFile(String localFilePath, String destFilePath) throws IOException{

		//打开HDFS目标文件的输出流		hdfs://master:9000/count/input/wordCount.txt
		FSDataOutputStream os = fs.create(new Path(destFilePath));
		
		//打开本地文件的输入流，以字节流形式写到目标文件输出流		"/home/hadoop/tmp/fileToHDFS.txt"
		FileInputStream is = new FileInputStream(localFilePath);
		
		IOUtils.copy(is, os);
	}
	
	/**
	 * get  下载HDFS文件 至 本地
	 * @param destFilePath	待下载的HDFS文件路径
	 * @param localFilePath	本地文件保存路径
	 */
	public static void downloadFile(String destFilePath, String localFilePath) throws IllegalArgumentException, IOException{
		
		//打开HDFS文件的输入流
		FSDataInputStream is = fs.open(new Path(destFilePath));
		
		//打开本地文件的输出流，将HDFS文件的输入流以字节流形式写到本地文件输出流
		FileOutputStream os = new FileOutputStream(localFilePath);
		IOUtils.copy(is, os);
		
	}
	
	
	
	
}
