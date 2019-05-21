
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.*;

import java.io.IOException;

import java.time.ZoneId;
import java.util.List;

public class Query1 {

    public static void getResponse(SparkSession spark, String pathWeather, List<ZoneId> zoneIdList,String format) throws IOException {


        Dataset<Row> weather_file=null;
        weather_file=spark.read().format(format).load(pathWeather);
        JavaRDD<Row> weather_info=weather_file.toJavaRDD();
        Row header=weather_info.first();
        String[] citiesList=new String[header.length()-1];
        for(int x=1;x<header.length();x++){
                citiesList[x-1]=header.get(x).toString();
            }

        JavaRDD<Row> r = weather_info.filter(y -> !y.equals(header));
        List<Row> l = r.collect();


        JavaPairRDD<String, Integer> weather_infoJavaRDD=weather_info.filter(x->FileInfoParser.check(x,header))
                .flatMapToPair(line->FileInfoParser.parseTemp(line,citiesList,zoneIdList))
                .filter(x->x._1().split("-")[1].equals("03") || x._1().split("-")[1].equals("04") || x._1().split("-")[1].equals("05") )
                .reduceByKey((k,z)->k+z)
                .filter(f->f._2()>=18)
                .mapToPair(p->new Tuple2<>(FileInfoParser.getKey(p._1()),1))
                .reduceByKey((x,y)->x+y).filter(p->p._2()>=15)
                .mapToPair(p->new Tuple2<>(FileInfoParser.getKey2(p._1()),1))
                .reduceByKey((x,y)->x+y).filter(p->p._2()>=3);
        JavaPairRDD<String, Iterable<String>> results= weather_infoJavaRDD.mapToPair(p-> new Tuple2<>(p._1().split("_")[1],p._1().split("_")[0])).groupByKey();

        results.saveAsTextFile(Main.hdfs_uri+"/user/query1");

        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - Main.startTime;
        System.out.println("Execution time in seconds: " + timeElapsed / 1000);

        HBaseUtils.execute("/user/query1/part-00000",1,-1,Main.hdfs_uri);


    }
}
