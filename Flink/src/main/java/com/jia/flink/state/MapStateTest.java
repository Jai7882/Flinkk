package com.jia.flink.state;

import com.jia.flink.pojo.WaterSensor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * ClassName: ValueStateTest
 * Package: com.jia.flink.state
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 20:47
 * @Version 1.0
 */
public class MapStateTest {

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
							private MapState<Integer,String> mapState;

							@Override
							public void open(Configuration parameters) throws Exception {
								MapStateDescriptor<Integer, String> mapStateDescriptor = new MapStateDescriptor<>("mapState", Types.INT, Types.STRING);
								mapState = getRuntimeContext().getMapState(mapStateDescriptor);
							}

							@Override
							public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
								mapState.put(value.getVc(),"test");
								out.collect("传感器:" +value.getId()+",水位记录"+mapState.keys());
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
