package com.jia.flink.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: BroadcastStateTest
 * Package: com.jia.flink.state
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 20:49
 * @Version 1.0
 */
public class BroadcastStateTest {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000);
		DataStreamSource<String> dataDs = env.socketTextStream("hadoop102", 8888);
		MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("mapState", Types.STRING, Types.STRING);
		BroadcastStream<String> broadcastDs = env.socketTextStream("hadoop102", 9999)
				.broadcast(stateDescriptor);

		dataDs.connect(broadcastDs)
				.process(new BroadcastProcessFunction<String, String, String>() {
					@Override
					public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
						String math = ctx.getBroadcastState(stateDescriptor).get("math");
						if ("1".equals(math)){
							out.collect("执行1号计算");
						}else {
							out.collect("执行默认计算");
						}
						out.collect(value);
					}

					@Override
					public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
						BroadcastState<String, String> conf = ctx.getBroadcastState(stateDescriptor);
						conf.put("math",value);
					}
				}).print();


		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
