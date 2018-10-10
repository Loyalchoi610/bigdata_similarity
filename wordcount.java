import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.sources.In;
import scala.Int;
import scala.Tuple2;


public class wordcount {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("All pair partitioning");

        JavaSparkContext ctx = new JavaSparkContext(conf);

        String filePath = args[0];
        double THRESHOLD = Double.parseDouble(args[1]);
        JavaRDD<String> data = ctx.textFile(filePath);

        // Map
        //input
        //0 1
        //0 2
        //0 3
        //1 3
        //1 4
        //1 5
        //output
        // 0 | 1 2 3
        // 1 | 3 4 5
        JavaPairRDD<Integer,Integer> init = data.flatMapToPair(p -> {
            List<Tuple2<Integer,Integer>> result = new ArrayList<>();
            // Fill in here
            String[] tmp = p.split("\t");
            result.add(new Tuple2<>(Integer.parseInt(tmp[0].trim()),Integer.parseInt(tmp[1].trim())));
            result.add(new Tuple2<>(Integer.parseInt(tmp[1].trim()),Integer.parseInt(tmp[0].trim())));
            return result.iterator();
        });
        //Reduce를 해준것을다시 Map
        //output
        //1 | <0,1>,<0,2><0,3>
        JavaPairRDD<Integer,Tuple2<Integer,Integer>> makeInvertedIndex = init.groupByKey().flatMapToPair(g -> {
            List<Tuple2<Integer,Tuple2<Integer,Integer>>> result = new ArrayList<>();
            for (Integer x : g._2()) {
                for(Integer y: g._2()){
                    result.add(new Tuple2<>(x,new Tuple2<>(g._1(),y)));
                }

            }
            return result.iterator();
        });
        //각 Relation당 size 구하기
        JavaPairRDD<Integer,Integer> sizeOfInvertedIndex = init.groupByKey().flatMapToPair(g -> {
            int count=0;
            int before =-1;
            List<Tuple2<Integer,Integer>> result = new ArrayList<>();
            for (Integer x : g._2()) {
                result.add(new Tuple2<>(g._1(),1));
            }
            return result.iterator();
        });
        JavaPairRDD<Integer, Integer> sizeCounts = sizeOfInvertedIndex.reduceByKey((a, b) -> a + b);

        // Reduce
        // output
        // key value
        JavaPairRDD<Tuple2<Integer,Integer>,Integer> jaccard = makeInvertedIndex.groupByKey().flatMapToPair(g -> {
            List<Tuple2<Tuple2<Integer,Integer>,Integer>> result = new ArrayList<>();
            ArrayList<Integer> keys = new ArrayList<>();
            HashMap<Integer,HashSet<Integer>> friendMap = new HashMap<>();
            int before =-1;
            for (Tuple2<Integer, Integer> p : g._2()) {
                if(before==-1 || before != p._1()){
                    keys.add(p._1());
                    before = p._1();
                    HashSet<Integer> hash = new HashSet<>();
                    hash.add(p._2());
                    friendMap.put(p._1(),hash);
                }
                else{
                    HashSet<Integer> hash = friendMap.get(p._1());
                    hash.add(p._2());
                }

            }
            Collections.sort(keys);
            int len = keys.size();
            if(len>1){
                for(int i=0; i<len-1; i++){
                    for (int j=i+1; j<len; j++){
//
                        if(!friendMap.get(keys.get(i)).contains(keys.get(j))){
                            result.add(new Tuple2<>(new Tuple2<>(keys.get(i),keys.get(j)),1));
                        }

                    }
                }
            }
            return result.iterator();
        });

//
//        // key value pair
//

       JavaPairRDD<Tuple2<Integer,Integer>, Integer> wordCounts = jaccard.reduceByKey((a, b) -> a + b);
        ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> result = new ArrayList<>();
        HashMap<Integer, Integer> sizeInfo = new HashMap<>();

        for (Tuple2<Integer,Integer> bb : sizeCounts.collect()) {
            sizeInfo.put(bb._1(),bb._2());

        }

        for (Tuple2<Tuple2<Integer,Integer>,Integer> aa : wordCounts.collect()) {
//            result.add(aa);
//            caculate jaccard

            if(aa._2()>THRESHOLD*(sizeInfo.get(aa._1()._1()) + sizeInfo.get(aa._1()._2()))/(1+THRESHOLD)){
                result.add(aa);
            }

        }
        Collections.sort(result, new Comparator<Tuple2<Tuple2<Integer, Integer>, Integer>>() {
            @Override
            public int compare(Tuple2<Tuple2<Integer, Integer>, Integer> o1, Tuple2<Tuple2<Integer, Integer>, Integer> o2) {
                int val = Integer.compare(o1._1()._1(),o2._1()._1());
                if(val==-1){
                    return -1;
                }else if(val==0){
                    return Integer.compare(o1._1()._2(), o2._1()._2());
                }else{
                    return 1;
                }
            }
        });
        int len = result.size();
        for(int i=0; i<len; i++){
            Tuple2<Tuple2<Integer,Integer>,Integer> temp = result.get(i);
            System.out.println(temp._1() + " " + temp._2() + " "
                    + THRESHOLD*(sizeInfo.get(temp._1()._1()) + sizeInfo.get(temp._1()._2()))/(1+THRESHOLD));
        }
        System.out.println("size : " + result.size());

        ctx.close();
    }
}
