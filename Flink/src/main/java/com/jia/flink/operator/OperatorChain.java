package com.jia.flink.operator;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: OperatorChain
 * Package: com.jia.flink.operator
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 10:45
 * @Version 1.0
 */
public class OperatorChain {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setString("rest.address","localhost");
        conf.setInteger("rest.port",5678);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);

        ds.map(String::toUpperCase).name("map1")
                .map(String::toLowerCase).name("map2")
                .map(s->s).name("map3")
                .print();




        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }



}
