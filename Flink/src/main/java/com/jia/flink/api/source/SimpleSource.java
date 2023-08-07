package com.jia.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * ClassName: SimpleSource
 * Package: com.jia.flink.source
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 9:02
 * @Version 1.0
 */
public class SimpleSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<String> stringList = Arrays.asList("a", "b", "c", "a", "e");
        DataStreamSource<String> ds1 = env.fromCollection(stringList);

        DataStreamSource<Integer> ds2 = env.fromElements(5, 1, 3, 3, 5);
        ds2.print();

        DataStreamSource<String> ds3 = env.socketTextStream("hadoop102", 8888);
        ds3.print();

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
