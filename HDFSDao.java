package com.cp.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HDFSDao {
	
	//配置信息封装对象,用于配置HDFS相关信息，提供HDFS文件系统的客户端的实例化
	private static Configuration conf = new Configuration();		//构造函数中会对classpath下的xxx.site.xml文件进行解析，在真实项目环境下，应该把xxx.site.xml加入到工程中
	private static FileSystem fs = null;		//HDFS文件系统的客户端对象，通过它来操作HDFS文件系统
	
	/**
	 * 获取HDFS文件系统客户端 
	 */
	public static void init() throws IOException{
		
		//设置HDFS文件系统相关配置参数
		conf.set("fs.defaultFS", "hdfs://master:9000/");
		conf.set("df.rep","2");
		fs = FileSystem.get(conf);		//根据配置文件的配置信息，实例化获取到对应配置文件类型的文件系统客户端
	}
	
	/**
	 *  增copyFromLocal   上传文件 至 HDFS
	 * @param localFilePath	待上传的文件路径
	 * @param destFilePath	上传至HDFS的文件路径
	 */
	public static void uploadFile(String localFilePath, String destFilePath) throws IllegalArgumentException, IOException{
		
		File f = new File(localFilePath);
		if(f.exists()){
			fs.copyFromLocalFile(new Path(localFilePath), new Path(destFilePath));
		}else{
			System.out.println("待上传的文件不存在！");
			System.exit(0);
		}
		
	}

	/**
	 *  增copyToLocal  下载文件至本地
	 * @param destFilePath	待下载的文件路径
	 * @param localFilePath	下载至本地的文件路径
	 */
	public static void downloadFile(String destFilePath, String localFilePath) throws IllegalArgumentException, IOException{
		
		checkFile(destFilePath);
		fs.copyToLocalFile(new Path(destFilePath), new Path(localFilePath));
	}

	/**
	 *  增mkdirs  创建一个文件目录，若父目录不存在，则递归创建
	 * @param dirPath	待创建文件目录路径
	 */
	public static void makeDir(String dirPath) throws IllegalArgumentException, IOException{
		boolean res = fs.mkdirs(new Path(dirPath));
		System.out.println(res?"创建成功！":"创建失败！");
		
	}
	
	/**
	 *  删delete
	 * @param destFilePath	待删除目标文件路径,若是删除目录，则递归删除文件目录
	 */
	public static void deleteFile(String destFilePath) throws IllegalArgumentException, IOException{
		
		checkFile(destFilePath);
		boolean res = fs.delete(new Path(destFilePath), true);
		System.out.println(res?"删除成功！":"删除失败！");
	}
	
	
	/**
	 *  改rename  重命名文件或目录
	 * @param oldFileNamePath	原文件/目录路径
	 * @param newFileNamePath	新文件/目录路径
	 */
	public static void rename(String oldFileNamePath, String newFileNamePath) throws IllegalArgumentException, IOException{
		
		checkFile(oldFileNamePath);
		boolean res = fs.rename(new Path(oldFileNamePath), new Path(newFileNamePath));
		System.out.println(res?"重命名成功！":"重命名失败！");
	}

	/**
	 *  查listFiles 查看指定目录下的文件，若目录下还有目录则递归查看
	 * @param dirPath	待查看的目录路径
	 * ps 只查看文件
	 */
	public static void listFiles(String dirPath) throws FileNotFoundException, IllegalArgumentException, IOException{
		
		checkFile(dirPath);
		//listFiles方法返回的就是一个存放“LocatedFileStatus”的迭代器
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(dirPath), true);
		
		//直接通过迭代器遍历目录下的文件
		while(listFiles.hasNext()){
			
			LocatedFileStatus file = listFiles.next();
			System.out.println(file.getPath().getName());
		}
	}
	
	/**
	 *  查listStatus 查看指定目录下的文件或目录，若目录下还有目录，不再递归查看
	 * @param dirPath 待查看的目录路径
	 */
	public static void listDir(String dirPath) throws FileNotFoundException, IllegalArgumentException, IOException{
		
		checkFile(dirPath);
		//listStatus方法返回的就是一个FileStatus数组
		FileStatus[] status = fs.listStatus(new Path(dirPath));
		
		//通过FileStatus数组遍历目录下的文件和目录信息
		for(FileStatus file : status){
			
			System.out.println((file.isDirectory() ? "目录" : "文件") + " " + file.getPath().getName());
		}
	}
	
	
	private static void checkFile(String filePath) throws IllegalArgumentException, IOException{
		if( !fs.exists(new Path(filePath)) ){
			System.out.println("文件或目录不存在！");
			System.exit(0);
		}
	}
	
	
	public static void list(String path) throws IllegalArgumentException, IOException{
		
		checkFile(path);
		FileStatus[] files = fs.listStatus(new Path(path));
		for(FileStatus file : files){
			
			if(file.isDirectory()){
				
				System.out.println( "-"+file.getPath().getName());
				list(file.getPath().toString());
				
			}else if(file.isFile()){
				System.out.println(file.getPath().getName());
				
			}else{
				
			}
		}
		
		
		
	}
	//exists  concat deleteOnExit append getContentSummary geFileBlockLoations getDefault** getFileStatus listFiles list***
	
}
