
import net.iakovlev.timeshape.TimeZoneEngine;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.sources.In;
import scala.Tuple2;
import utils.*;

import java.text.ParseException;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Query1 {

    public static void getResponse(JavaSparkContext sc, String pathWeather, List<ZoneId> zoneIdList, HashMap<String,String> hmapCities) {

        JavaRDD<String> weather_info= sc.textFile(pathWeather);

        String header=weather_info.first();
        String[] citiesList = Arrays.copyOfRange(header.split(","), 1, header.split(",").length);


        JavaPairRDD<String, Integer> weather_infoJavaRDD=weather_info.filter(y->!y.equals(header))
                .flatMapToPair(line->WeatherInfoParser.parseCsv2(line,citiesList,zoneIdList))
                .filter(x->x._1().split("-")[1].equals("03") || x._1().split("-")[1].equals("04") || x._1().split("-")[1].equals("05") )
                .reduceByKey((k,z)->k+z)
                .filter(f->f._2()>=18)
                .mapToPair(p->new Tuple2<>(WeatherInfoParser.getKey(p._1()),1))
                .reduceByKey((x,y)->x+y).filter(p->p._2()>=15)
                .mapToPair(p->new Tuple2<>(WeatherInfoParser.getKey2(p._1()),1))
                .reduceByKey((x,y)->x+y).filter(p->p._2()>=3);
        JavaPairRDD<String, Iterable<String>> results= weather_infoJavaRDD.mapToPair(p-> new Tuple2<>(p._1().split("_")[1],p._1().split("_")[0])).groupByKey();

        results.saveAsTextFile("output_query1");


    }
}
