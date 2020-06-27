package util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ListSort {


    public List<Tuple2<String,Long>>  sort(List<Tuple2<String,Long>> list){
        List<Tuple2<String,Long>> listSort = new ArrayList<>();
        for (Tuple2<String,Long> tuple2: list) {
            Long aa =  tuple2.f1;


        }

        return  null  ;


    }



}
