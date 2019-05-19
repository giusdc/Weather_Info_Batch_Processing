import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.*;

import java.io.File;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static String hdfs_uri = "hdfs://35.159.37.27:8020";

    private static String[] pathList = {hdfs_uri + "/user/hdfs/Temperature.csv", hdfs_uri + "/user/hdfs/Pressure.csv", hdfs_uri + "/user/hdfs/Humidity.csv", hdfs_uri + "/user/hdfs/City_attributes.csv", hdfs_uri + "/user/hdfs/Weather.csv"};
    private static String[] pathListAvro = {hdfs_uri + "/user/hdfs/Temperature.avro", hdfs_uri + "/user/hdfs/Pressure.avro", hdfs_uri + "/user/hdfs/Humidity.avro", hdfs_uri + "/user/hdfs/City_attributes.avro", hdfs_uri + "/user/hdfs/Weather.avro"};

    //private static String pathAvro="avro/cityAttributes.avro";
    public static long startTime;


    public static void main(String[] args) throws Exception {

        //CsvToAvroConverter.converter(pathList, pathToCityFile);

        //UTCUtils.read();
        //HDFSUtils.init(pathList);

        //starting timer
        startTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hadoopconf = sc.hadoopConfiguration();
        hadoopconf.set("fs.defaultFS", hdfs_uri);
        hadoopconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hadoopconf.set("dfs.permissions", "false");
        sc.setLogLevel("ERROR");

        SparkSession spark = SparkSession.builder()
                .appName("App")
                .getOrCreate();
        Dataset<Row> avroInput = spark.read().format("avro").load(pathListAvro[3]);
        JavaRDD<Row> avroRow = avroInput.toJavaRDD();
        List<Row> ko = avroRow.collect();
        System.out.printf(ko.get(0).toString());


        //Get mapping city->country
        if (false) {

            JavaRDD<String> city_info = sc.textFile(pathList[3]);
            //JavaRDD<String> city_info = sc.textFile(pathAvro);

            String header = city_info.first();
            

            /* for avro
            JavaPairRDD<String, String> cityCountryMapRDD = avroRow.mapToPair(c -> new Tuple2<>(CityParser.parseAvro(c).getCity(), CountryMap.sendGet(c))).cache();
            JavaPairRDD<String, Float[]> cityCoordinateRDD = city_info.filter(y -> !y.equals(header)).mapToPair(c -> new Tuple2<>(CityParser.parseCsv(c).getCity(), new Float[]{Float.parseFloat(CityParser.parseCsv(c).getLatitude()), Float.parseFloat(CityParser.parseCsv(c).getLongitude())}));
             */


            JavaPairRDD<String, String> cityCountryMapRDD = city_info.filter(y -> !y.equals(header)).mapToPair(c -> new Tuple2<>(CityParser.parseCsv(c).getCity(), CountryMap.sendGet(c))).cache();
            JavaPairRDD<String, Float[]> cityCoordinateRDD = city_info.filter(y -> !y.equals(header)).mapToPair(c -> new Tuple2<>(CityParser.parseCsv(c).getCity(), new Float[]{Float.parseFloat(CityParser.parseCsv(c).getLatitude()), Float.parseFloat(CityParser.parseCsv(c).getLongitude())}));

            Map<String, String> map = cityCountryMapRDD.collectAsMap();
            HashMap<String, String> hmapCities = new HashMap<String, String>(map);
            List<Float[]> values = cityCoordinateRDD.values().collect();
            List<String> cityList = cityCoordinateRDD.keys().collect();
            String[] citiesName = cityList.toArray(new String[cityList.size()]);

            //Get mapping pair zoneId/cities
            List<Tuple2<String, ZoneId>> mapping = UTCUtils.getZoneId(values, citiesName);
            JavaRDD rdd = sc.parallelize(mapping);
            JavaPairRDD<String, ZoneId> mappingPair = JavaPairRDD.fromJavaRDD(rdd).cache();
            // mappingPair.saveAsTextFile("hdfs://3.122.52.163:8020/user/prova");
            List<ZoneId> zoneIdList = mappingPair.values().collect();

            int query = Integer.parseInt(args[0]);

            switch (query) {
                case 1:
                    /*  Process Query 1 */
                    Query1.getResponse(sc, pathList[4], zoneIdList, hmapCities/*useless*/);
                    break;

                case 2:
                    /*  Process Query 2 */
                    Query2.getResponse(sc, pathList, zoneIdList, hmapCities);
                    break;

                case 3:
                    /*  Process Query 3 */
                    Query3.getResponse(sc, pathList[0], zoneIdList, hmapCities);
                    break;
                case 4:
                    Query1.getResponse(sc, pathList[4], zoneIdList, hmapCities);
                    Query2.getResponse(sc, pathList, zoneIdList, hmapCities);
                    Query3.getResponse(sc, pathList[0], zoneIdList, hmapCities);
                default:
                    break;

            }
            sc.stop();

        }


    }
}
