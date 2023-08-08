package com.jia.flink.api.transform;

import com.jia.flink.api.function.ClickSource;
import com.jia.flink.pojo.AppEvent;
import com.jia.flink.pojo.Event;
import com.jia.flink.pojo.ThirdPartyEvent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * ClassName: BillCheck
 * Package: com.jia.flink.api.transform
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 19:57
 * @Version 1.0
 */
public class BillCheck {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<AppEvent> appDs = env.socketTextStream("hadoop102", 8888).map(line -> {
			String[] words = line.split(",");
			return new AppEvent(words[0].trim(), words[1].trim(), Long.valueOf(words[2].trim()));
		});

		SingleOutputStreamOperator<ThirdPartyEvent> thirdPartyDs = env.socketTextStream("hadoop102", 9999).map(line -> {
			String[] words = line.split(",");
			return new ThirdPartyEvent(words[0].trim(), words[1].trim(), words[2].trim(), Long.valueOf(words[3].trim()));
		});

		ConnectedStreams<AppEvent, ThirdPartyEvent> connect = appDs.connect(thirdPartyDs);
		SingleOutputStreamOperator<String> resultDs = connect.process(new CoProcessFunction<AppEvent, ThirdPartyEvent, String>() {
			HashMap<String, AppEvent> appEventMap = new HashMap<>();
			HashMap<String, ThirdPartyEvent> thirdPartyEventMap = new HashMap<>();

			@Override
			public void processElement1(AppEvent value, CoProcessFunction<AppEvent, ThirdPartyEvent, String>.Context ctx, Collector<String> out) throws Exception {
				if (thirdPartyEventMap.get(value.getOrderId()) != null) {
					// 对账成功
					thirdPartyEventMap.remove(value.getOrderId());
					out.collect(value.getOrderId() + "对账成功！！！ ThirdParty早到，App晚到！！");
				} else {
					appEventMap.put(value.getOrderId(), value);
				}
			}

			@Override
			public void processElement2(ThirdPartyEvent value, CoProcessFunction<AppEvent, ThirdPartyEvent, String>.Context ctx, Collector<String> out) throws Exception {
				if (thirdPartyEventMap.get(value.getOrderId()) != null) {
					// 对账成功
					appEventMap.remove(value.getOrderId());
					out.collect(value.getOrderId() + "对账成功！！！ ThirdParty晚到，App早到！！");
				} else {
					thirdPartyEventMap.put(value.getOrderId(), value);
				}
			}
		});
		resultDs.print("对账");
		try {
			env.execute();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


}
