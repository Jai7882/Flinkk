package com.jia.flink.state;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: BillCheckV2
 * Package: com.jia.flink.state
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/12 13:48
 * @Version 1.0
 */
public class BillCheckV2 {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<Event> ds = env.addSource(new ClickSource());
		ds.print();

		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
