package util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * autor:likui
 *
 */
public class windowUtil implements WindowFunction<Tuple2<String,Long>, Tuple6<String,Integer,String,String,String,String>,String, TimeWindow> {


    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> input,
                      Collector<Tuple6<String, Integer, String, String,String,String>> out) throws Exception {
        List<Tuple2<String,Long>> list = new ArrayList<>();
        for (Tuple2<String,Long> tuple2 : input) {
            list.add(tuple2);

        }
        List<Tuple2<String,Long>> list1 = new ArrayList<>();
        list.sort((tuple1, tuple2) -> (int)(tuple1.f1-tuple2.f1));
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        int size = list.size();
        /*out.collect(key,list.size(),format.format(list.get(0)),
                format.format(list.get(1)),format.format(timeWindow.getStart())
                ,format.format(timeWindow.getEnd()));*/
        Tuple6<String, Integer, String, String,String,String> tuple6 = new Tuple6<>();
        tuple6.setFields(key,list.size(),format.format(list.get(0)),
                format.format(list.get(1)),format.format(timeWindow.getStart())
                ,format.format(timeWindow.getEnd()));
        out.collect(tuple6);



    }


    public static void main(String[] args) {

       /* Tuple2<String, Long> tuple2 = new Tuple2<String, Long>();
        Tuple2<String, Long> tuple3 = new Tuple2<String, Long>();
        Tuple2<String, Long> tuple1 = new Tuple2<String, Long>();
        tuple2.setFields("wwww",10000L);
        tuple3.setFields("ccccc",20000L);
        tuple1.setFields("bbbbb",20L);
        List<Tuple2<String,Long>> list = new ArrayList<>();
        list.add(tuple2);
        list.add(tuple3);
        list.add(tuple1);

        System.out.println(list);
        list.sort(new Comparator<Tuple2<String, Long>>() {
            @Override
            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                return (int) (o1.f1 - o2.f1);
            }
        });
        System.out.println(list.get(0));*/

        //long l = System.currentTimeMillis();

        long l = 1461756862000L;
        System.out.println("====:"+l);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String format1 = format.format(l);


        System.out.println(format1);


        /**
         * 00001,1593224122000
         *
         * 00001,1593224126000
         * 00001,1593224132000
         * 00001,1593224133000
         * 00001,1593224134000
         *
         */


        //System.out.println(l);


    }

}
