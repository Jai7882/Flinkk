package com.jia.flink.state;

import com.jia.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class AggregatingStateTest {

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
							private AggregatingState<Integer, Double> aggregatingState;

							@Override
							public void open(Configuration parameters) throws Exception {
								AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> aggregatingStateDescriptor = new AggregatingStateDescriptor<>("aggregatingState",
										new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
											@Override
											public Tuple2<Integer, Integer> createAccumulator() {
												return Tuple2.of(0, 0);
											}

											@Override
											public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
												return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
											}

											@Override
											public Double getResult(Tuple2<Integer, Integer> accumulator) {
												return accumulator.f0.doubleValue()/accumulator.f1;
											}

											@Override
											public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
												return null;
											}
										},
										Types.TUPLE(Types.INT, Types.INT));
								aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
							}

							@Override
							public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
								aggregatingState.add(value.getVc());
								out.collect(value + ", 平均水位:" + aggregatingState.get());
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
