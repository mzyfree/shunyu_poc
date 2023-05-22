package com.bytedance.demos;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

/**
 * @author minzhongyuan@bytedance.com
 * @created 22/05/2023
 * @project shunyu_poc
 * @description subscribe description
 */
public class ProcessTimeDemo {
  private static final Integer TIME = 5 * 1000;

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);

    env.addSource(new LateRecordSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .forGenerator(context -> new LateRecordGenerator())
                .withTimestampAssigner((v, timestamp) -> v.f1))
        .keyBy(v -> v.f0)
        .process(new ProcessFunction<Tuple2<String, Long>, Object>() {

          @Override
          public void processElement(Tuple2<String, Long> stringLongTuple2, Context context,
              Collector<Object> collector) throws Exception {
            long waterMark = context.timerService().currentWatermark();
            System.out.println(String.format(
                "Accepted record: %s, Current Watermark: %s, Current Event Time: %s, Process Time: %s.",
                stringLongTuple2, waterMark, stringLongTuple2.f1, System.currentTimeMillis() - stringLongTuple2.f1
            ));
            collector.collect(stringLongTuple2);
          }

        })
        .name("source").addSink(new SinkFunction<Object>() {
      // do nothing.
    });

    env.execute("test-hmk");
  }

  public static class LateRecordSource extends RichSourceFunction<Tuple2<String, Long>> {

    private volatile long id = 0L;

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
      while (running) {
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.collect(Tuple2.of("test-" + id, System.currentTimeMillis()));
          id++;
        }
        Thread.sleep(TIME);
      }
    }

    @Override
    public void cancel() {
      this.running = false;
    }
  }

  public static class LateRecordGenerator implements WatermarkGenerator<Tuple2<String, Long>> {

    private long maxTimestamp = 0;

    @Override
    public void onEvent(Tuple2<String, Long> stringLongTuple2, long l,
        WatermarkOutput watermarkOutput) {
      maxTimestamp = stringLongTuple2.f1;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
      watermarkOutput.emitWatermark(new Watermark(maxTimestamp));
    }
  }

}
