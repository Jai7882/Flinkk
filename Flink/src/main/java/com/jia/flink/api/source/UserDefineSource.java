package com.jia.flink.api.source;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: UserDefineSource
 * Package: com.jia.flink.api.source
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 14:56
 * @Version 1.0
 */
public class UserDefineSource {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		ClickSource source = new ClickSource();
		DataStreamSource<Event> ds = env.addSource(source);

		ds.print();

		//	通过另外一个线程控制数据生成终止
		new Thread(new Runnable() {

			boolean isRunning = true;

			/*
			持续监控用户意图
			根据用户意图决定是否停止数据生成 将isRunning修改为false
			 */
			@Override
			public void run() {

				try {
					FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), new Configuration(), "atguigu");
					while (isRunning) {
						boolean exists = fs.exists(new Path("hdfs://hadoop102:8020/cancel"));
						if (exists) {
							source.cancel();
							isRunning = false;
						}
						TimeUnit.SECONDS.sleep(1);
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).start();


		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

}
