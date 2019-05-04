package Query2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.*;

import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query2 {


    private static String pathToCityFile = "data/city_attributes.csv";
    private static String temperaturePath= "data/temperature.csv";


    public static void main(String[] args) throws ParseException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query2");
        JavaSparkContext sc = new JavaSparkContext(conf);


        //Get mapping city->country
        JavaRDD<String> city_info= sc.textFile(pathToCityFile);
        String header=city_info.first();
        JavaPairRDD<String, String> cityInfoRDD=city_info.filter(y->!y.equals(header)).mapToPair(c->new Tuple2<>(CityParser.parseCsv(c).getCity(),CountryMap.sendGet(c))).cache();

        Map<String,String> map =  cityInfoRDD.collectAsMap();
        HashMap<String,String> hmapCities = new HashMap<String,String>(map);

        //Get temperature values
        JavaRDD<String> tempInfo= sc.textFile(temperaturePath);
        String headerTemp=tempInfo.first();
        String[] citiesList = Arrays.copyOfRange(headerTemp.split(","), 1, headerTemp.split(",").length);

       //List<String> pippo= cityInfoRDD.lookup("Portland");




        JavaPairRDD<String,Float> tempInfoRDD=tempInfo.filter(y->!y.equals(headerTemp)).
                flatMapToPair(line-> TempInfoParser.parseCsv(line,citiesList,hmapCities));

        //List<TempInfo> ti=tempInfoRDD.take(15);

        System.out.println("ciao");







        tempInfoRDD.saveAsTextFile("output_query2");
















        sc.stop();
    }








}
