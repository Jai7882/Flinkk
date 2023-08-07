package com.jia.flink.api.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: FileSource
 * Package: com.jia.flink.api.source
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 13:05
 * @Version 1.0
 *
 * FileConnector 从文件中读数据或者将数据写入到文件中
 *  FileSource
 *  FileSink
 *
 *  Flink Source API
 *   1  env.fromSource(Source,watermarkStrategy,String) 新api
 *   2  env.addSource() 旧api
 *
 */
public class FileSource {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.setString("rest.address", "localhost");
		conf.setInteger("rest.port", 5678);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
		env.setParallelism(1);
		FileSource

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}



}
