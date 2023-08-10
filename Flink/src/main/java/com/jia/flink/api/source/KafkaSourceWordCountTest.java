package com.jia.flink.api.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * ClassName: KafkaSource
 * Package: com.jia.flink.source
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 8:37
 * @Version 1.0
 */
public class KafkaSourceWordCountTest {

    //1 并行度是cpu同时调度多线程 可以由执行环境对象setParallelism或者在linux启动配置或者提交任务时设置
    // 还可以在flink配置文件中修改并行度

    //2 算子链是flink中一个数据流在算子之间传输的形式，合并算子链的条件是并行度相同 默认是开启合并算子链的
    // 可以通过算子的disableChaining()禁用算子链

    //3 一个任务中并行度取决于整个任务中并行度最大的，也就是这次任务重需要的slot的数量

    //4 通过yarn的rm分配资源启动applicationmaster 再通过am上通信系统启动分发器和资源管理器
    // 然后通过分发器启动jobmaster 再去生成flink可以识别的执行作业图 再去通过jobmaster创建taskmaster
    // taskmaster创建所需要的taskslot然后由yarn的rm启动taskmaster


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> ks = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("flink")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("topic_a")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<String> ds = env.fromSource(ks, WatermarkStrategy.noWatermarks(),"kafkaSource");
        SingleOutputStreamOperator<Tuple2<String,Long>> flatMap = ds.flatMap((String line, Collector<Tuple2<String,Long>> collector) -> {
            String[] words = line.split(",");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1L));
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> returns = flatMap.returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> keyBy = returns.keyBy(v -> v.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyBy.sum(1);
        sum.print();
        env.execute();

    }

}
