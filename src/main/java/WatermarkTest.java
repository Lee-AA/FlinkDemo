import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import util.windowUtil;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;

/**
 * @author:likui
 * @date:2020.6.25
 * @FLINK_version: FLINk1.10
 * @realize:watermark+window解决数据乱序问题
 */
public class WatermarkTest {


    public static void main(String[] args)  {
        //String hostName="192.168.253.80";
        //int port=123;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //定义时间特征
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从socket上获取数据
        DataStreamSource<String> input = env.socketTextStream("192.168.253.80", 9000,"\n");

        //将数据转成元组类型
        SingleOutputStreamOperator<Tuple2<String, Long>> inputMap = input.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2 map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2(arr[0], Long.valueOf(arr[1]));
            }
        });

        //inputMap.print();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        //watermark包括 Periodic Watermarks和 Punctuated Watermarks
        //设置水位，这里采用的是 Periodic Watermarks
        //抽取timestamp生成watermark。
        // 并打印（code，time，格式化的time，currentMaxTimestamp，currentMaxTimestamp的格式化时间，watermark时间）
        SingleOutputStreamOperator<Tuple2<String, Long>> watermark = inputMap.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            Long currentMaxTimestamp = 0L;
            Long maxOutOfOrderness = 10000L;//最大允许的乱序时间是10s
            Watermark a = null;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                a = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                return a;
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                Long timestamp = tuple2.f1;
                currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
                System.out.println("timestamp:" + tuple2.f0 + "," + tuple2.f1 + "|" +
                        format.format(tuple2.f1) + "," + currentMaxTimestamp
                        + "|" + format.format(currentMaxTimestamp) + "," + a.toString());
                return timestamp;
            }
        });

        //watermark.print();

        //Event Time每3s触发一次窗口 输出（code，窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间）
        SingleOutputStreamOperator<Tuple6<String, Integer, String, String, String, String>> window =
                watermark.keyBy(event -> event.f0).
                window(TumblingEventTimeWindows.of(Time.seconds(3))).apply(new windowUtil());

        window.print();

        try {
            env.execute("WatermarkTest");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
