package com.jia.flink.state;

import com.jia.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * ClassName: ValueStateTest
 * Package: com.jia.flink.state
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 20:47
 * @Version 1.0
 */
public class ListStateTest {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 8888)
				.map(line -> {
					String[] words = line.split(",");
					return new WaterSensor(words[0], Integer.valueOf(words[1]), Long.valueOf(words[2]));
				});

		ds.keyBy(WaterSensor::getId)
				.process(
						new KeyedProcessFunction<String, WaterSensor, String>() {
							private ListState<Integer> listState;

							@Override
							public void open(Configuration parameters) throws Exception {
								ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<>("listState", Types.INT);
								listState = getRuntimeContext().getListState(listStateDescriptor);
							}

							@Override
							public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
								listState.add(value.getVc());
								ArrayList<Integer> vcs = new ArrayList<>();
								for (Integer integer : listState.get()) {
									vcs.add(integer);
								}

								if (vcs.size()>3){
									vcs.sort(Integer::compare);
									vcs.remove(0);
								}
								listState.update(vcs);
								out.collect(value + "最高的三个水位ID:" + vcs);
							}
						}
				).print();


		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
