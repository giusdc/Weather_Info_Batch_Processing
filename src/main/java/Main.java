import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.CityParser;
import utils.CountryMap;
import utils.UTCUtils;

import java.text.ParseException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static String pathToCityFile = "data/city_attributes.csv";
    private static String[] pathList={"data/temperature.csv","data/pressure.csv","data/humidity.csv"};


    public static void main(String[] args) throws ParseException {

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Get mapping city->country
        JavaRDD<String> city_info= sc.textFile(pathToCityFile);
        String header=city_info.first();
        JavaPairRDD<String, String> cityCountryMapRDD=city_info.filter(y->!y.equals(header)).mapToPair(c->new Tuple2<>(CityParser.parseCsv(c).getCity(),CountryMap.sendGet(c))).cache();
        JavaPairRDD<String, Float[]> cityCoordinateRDD=city_info.filter(y->!y.equals(header)).mapToPair(c->new Tuple2<>(CityParser.parseCsv(c).getCity(),new Float[]{Float.parseFloat(CityParser.parseCsv(c).getLatitude()),Float.parseFloat(CityParser.parseCsv(c).getLongitude())}));

        Map<String,String> map =  cityCountryMapRDD.collectAsMap();
        HashMap<String,String> hmapCities = new HashMap<String,String>(map);
        List<Float[]> values = cityCoordinateRDD.values().collect();
        List<String> cityList = cityCoordinateRDD.keys().collect();
        String[] citiesName = cityList.toArray(new String[cityList.size()]);

        //Get mapping pair zoneId/cities
        List<Tuple2<String, ZoneId>> mapping = UTCUtils.getZoneId(values, citiesName);
        JavaRDD rdd = sc.parallelize(mapping);
        JavaPairRDD<String,ZoneId> mappingPair = JavaPairRDD.fromJavaRDD(rdd).cache();
        List<ZoneId> zoneIdList = mappingPair.values().collect();
        //calling Query2
        //Query2.getResponse(sc,pathList,zoneIdList,hmapCities);
        Query3.getResponse(sc,pathList[0],zoneIdList,hmapCities);

    }
}
