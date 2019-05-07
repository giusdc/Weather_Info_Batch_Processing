
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
import java.util.List;

public class Query1 {

    private static String pathToFile = "data/weather_description.csv";
    public static void main(String[] args) throws Exception {


        //UTCUtils.sendGet(35.084492f,-106.651138f,"2012-10-01 13:00:00");




        int i=0;

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String pathToCityFile = "data/city_attributes.csv";

        JavaRDD<String> city_info= sc.textFile(pathToCityFile);
        String header2=city_info.first();
        JavaPairRDD<String, Float[]> cityInfoRDD=city_info.filter(y->!y.equals(header2)).mapToPair(c->new Tuple2<>(CityParser.parseCsv(c).getCity(),new Float[]{Float.parseFloat(CityParser.parseCsv(c).getLatitude()),Float.parseFloat(CityParser.parseCsv(c).getLongitude())}));

        List<Float[]> values = cityInfoRDD.values().collect();
        JavaRDD<String> weather_info= sc.textFile("data/weather_description.csv");

        String header=weather_info.first();
        String[] citiesList = Arrays.copyOfRange(header.split(","), 1, header.split(",").length);

        //Get mapping pair zoneId/cities
        List<Tuple2<String, ZoneId>> mapping = UTCUtils.getZoneId(values, citiesList);
        JavaRDD rdd = sc.parallelize(mapping);
        JavaPairRDD<String,ZoneId> mappingPair = JavaPairRDD.fromJavaRDD(rdd).cache();
        List<ZoneId> zoneIdList = mappingPair.values().collect();

        JavaPairRDD<String, Integer> weather_infoJavaRDD=weather_info.filter(y->!y.equals(header)).
                flatMapToPair(line->WeatherInfoParser.parseCsv2(line,citiesList,zoneIdList)).
                filter(x->x._1().split("-")[1].equals("03") || x._1().split("-")[1].equals("04") || x._1().split("-")[1].equals("05") )
                .reduceByKey((k,z)->k+z)
                .filter(f->f._2()>=18)
                .mapToPair(p->new Tuple2<>(WeatherInfoParser.getKey(p._1()),1))
                .reduceByKey((x,y)->x+y).filter(p->p._2()>=15)
                .mapToPair(p->new Tuple2<>(WeatherInfoParser.getKey2(p._1()),1))
                .reduceByKey((x,y)->x+y).filter(p->p._2()>=3);
        JavaPairRDD<String, Iterable<String>> results= weather_infoJavaRDD.mapToPair(p-> new Tuple2<>(p._1().split("_")[1],p._1().split("_")[0])).groupByKey();

        results.saveAsTextFile("output_query1");









       /* for(Tuple2<String,Integer> pippo:count.collect()){
            System.out.println(pippo._1+" "+pippo._2);
        }*/






        sc.stop();
    }






}
