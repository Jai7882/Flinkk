package com.jia.flink.state;

import com.jia.flink.api.function.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: OperatorState
 * Package: com.jia.flink.state
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 20:16
 * @Version 1.0
 */
public class OperatorState {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000);
		DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);
		ds.map(new MyMapFunction())
				.addSink(new MySinkFunction2());


		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static class MyMapFunction implements MapFunction<String,String>, CheckpointedFunction {
		//列表状态
		private ListState<String> listState;
		@Override
		public String map(String value) throws Exception {
			listState.add(value);
			return listState.get().toString();
		}

		//状态持久化操作 flink托管
		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			System.out.println("Flink托管持久化");
		}

		//状态初始化
		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			System.out.println("正在初始化状态");
			ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<>("testState", Types.STRING);
			// 列表状态
//			listState = context.getOperatorStateStore().getListState(stateDescriptor);

			//联合列表状态
			listState = context.getOperatorStateStore().getUnionListState(stateDescriptor);
		}
	}

	private static class MySinkFunction2 implements SinkFunction<String> {

		@Override
		public void invoke(String value, Context context) throws Exception {
			System.out.println(value);
			if ("w".equals(value)) {
				throw  new RuntimeException("wwwwwwwwwwwwwwwwwwwwwww");
			}
		}
	}
}
