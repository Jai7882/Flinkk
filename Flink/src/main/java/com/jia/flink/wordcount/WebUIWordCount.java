package com.jia.flink.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ClassName: WebUIWordCount
 * Package: com.jia.flink.wordcount
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/4 10:28
 * @Version 1.0
 */
public class WebUIWordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.address","localhost");
        conf.setInteger("rest.port",5678);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置执行模式
        // STREAMING 流模式 默认值
        // BATCH     批处理
        // AUTOMATIC 自动选择 根据数据源是否有界 有界批处理 无界流处理
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 8888);
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = ds.flatMap((line, collector)->{
            String[] words = line.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word,1L));
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> returns = flatMap.returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> keyBy = returns.keyBy(value-> value.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyBy.sum(1);

        sum.print();
        env.execute();
    }

}
