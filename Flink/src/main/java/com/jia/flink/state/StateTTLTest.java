package com.jia.flink.state;

import com.jia.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ClassName: ValueStateTest
 * Package: com.jia.flink.state
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 20:47
 * @Version 1.0
 */
public class StateTTLTest {

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
							private ValueState<Integer> valueState;

							@Override
							public void open(Configuration parameters) throws Exception {
								ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("valueState", Types.INT);
								valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(5L))
										.setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
										.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
										.build());
								valueState = getRuntimeContext().getState(valueStateDescriptor);
							}

							@Override
							public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
								Integer lastVc = valueState.value();
								if (lastVc != null && lastVc < value.getVc()){
									out.collect(value + "水位记录相差超过10");
								}
								valueState.update(value.getVc());
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
