package com.jia.flink.api.transform;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: UserDefinePartition
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 19:04
 * @Version 1.0
 */
public class UserDefinePartition {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		ds.partitionCustom(new Partitioner<String>() {
			@Override
			public int partition(String key, int partitions) {
				if ("Zhang3".equals(key)||"Li4".equals(key)){
					return 0;
				}else {
					return 1;
				}

			}
		}, Event::getUser).print().setParallelism(2);


		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

}
