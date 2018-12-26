package cn.tledu.order;

import java.io.ByteArrayOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class One {

	public static void main(String[] args) throws Exception{
		String src="";
		String hdfs="";
		byte[] r=One.readFromFileToByteArray(src);
		One.writeHdfsFile(src, hdfs);
		System.out.println("成功了？？？");
		
	}
	  //linux wget 下载 www.baidu.com 首页的文本内容，命名为 index.html
	/*通过 java 代码读取本地的 index.html 文件成 java 字节数组。
	通过 java 代码打开 hdfs 要写入文件的输出流。
	通过 java 代码已经打开的输出流，写入之前读取完成的 java 字节数组。写入完成，将输出流关闭。
	通过 java 代码写主方法，传入本地的输入文件和 hdfs 的输出文件路径。
	maven 打包上传到开发环境，用 yarn jar 来执行。
	执行完成后查看传入 java 代码的 hdfs 的输出文件路径是否已存在，内容是否正确。
*/
     //(1)读取本地文件
	static Logger logger=Logger.getLogger(One.class);
	static Configuration conf=new Configuration();

	public static String readFromLocal(String src) throws Exception{
		if(StringUtils.isEmpty(src)){
			logger.error("路径不合法");
		}
		byte[] byteArry=readFromFileToByteArray(src);
		if(byteArry==null|| byteArry.length==0){
			return null;
		}
		return new String(byteArry,"utf-8");
	}
	
	//(2)读取文件为字节数组
	public static byte[] readFromFileToByteArray(String src) throws Exception{
		if(StringUtils.isEmpty(src)){
			 logger.error("路径不合法");
		}
		
		FileSystem fs=FileSystem.get(conf);
		Path localPath=new Path(src);
		
		FSDataInputStream fsInStream=fs.open(localPath);
		
		byte[] bytearr=new byte[65536];
		ByteArrayOutputStream baos=new ByteArrayOutputStream();
		
		int line=0;
		while((line=fsInStream.read(bytearr))!=-1){
			//???
			baos.write(bytearr);
			bytearr=new byte[65536];
		}
		
		fsInStream.close();
		
		
		byte[] result=baos.toByteArray();
		//fsOutStream.write(result);
		return result;
		
	}
	//(3)打开hdfs写文件输出流,写入字节数组
	public static void writeHdfsFile(String src,String hdfs)throws Exception{
		FileSystem fs=FileSystem.get(conf);
		Path hdfsPath=new Path(hdfs);
		FSDataOutputStream fsOutStream=fs.create(hdfsPath);
		byte[] result=readFromFileToByteArray(src);
		fsOutStream.write(result);
		
		fsOutStream.close();
	}

}
