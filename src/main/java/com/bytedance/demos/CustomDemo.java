package com.bytedance.demos;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author minzhongyuan@bytedance.com
 * @created 22/05/2023
 * @project shunyu_poc
 * @description subscribe description
 */
public class CustomDemo {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.setParallelism(1);
    //创建数据源
    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("192.168.45.194:9092")
        .setTopics("wordCountSource")
        .setGroupId("my-group")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();
    DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    // 数据处理
    OutputTag<String> dirtyDataTag=new OutputTag<String>("dirty_data"){};
    SingleOutputStreamOperator<Tuple2<String, Integer>> processStream = kafkaSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
      @Override
      public void processElement(String s, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        try {
          out.collect(new Tuple2<>("name", 1));
        } catch (Exception e) {
          ctx.output(dirtyDataTag, s);
        }
      }
    });
    SingleOutputStreamOperator<String> resultStream = processStream
        .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
          @Override
          public String getKey(Tuple2<String, Integer> s) throws Exception {
            return s.f0;
          }
        })
        .sum(1)
        .map(new MapFunction<Tuple2<String, Integer>, String>() {
          @Override
          public String map(Tuple2<String, Integer> s) throws Exception {
            return s.toString();
          }
        });
    //kafka输出
    KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers("192.168.45.194:9092")
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("wordCountSink")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
        )
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
    resultStream.sinkTo(sink);
    env.execute();
  }
}
