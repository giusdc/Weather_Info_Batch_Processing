import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.*;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static String hdfs_uri = "hdfs://35.158.98.173:8020";

    private static String[] pathListCsv = {hdfs_uri + "/user/hdfs/temperature.csv", hdfs_uri + "/user/hdfs/pressure.csv", hdfs_uri + "/user/hdfs/humidity.csv", hdfs_uri + "/user/hdfs/city_attributes.csv", hdfs_uri + "/user/hdfs/weather_description.csv"};
    //private static String[] pathListCsv = {"data/temperature.csv", "data/pressure.csv", "data/humidity.csv", "data/city_attributes.csv", "data/weather_description.csv"};
    private static String[] pathListAvro = {hdfs_uri + "/user/hdfs/temperature.avro", hdfs_uri + "/user/hdfs/pressure.avro", hdfs_uri + "/user/hdfs/humidity.avro", hdfs_uri + "/user/hdfs/city_attributes.avro", hdfs_uri + "/user/hdfs/weather_description.avro"};

    //private static String pathAvro="avro/cityAttributes.avro";
    public static long startTime;
    public static final int typeFile=0; //0 for csv,1 for avro,2 for parquet


    public static void main(String[] args) throws Exception {

        //CsvToAvroConverter.converter(pathListCsv, pathToCityFile);

        //UTCUtils.read();
        //HDFSUtils.init(pathListCsv);

        //starting timer
        startTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query");

        JavaSparkContext sc = new JavaSparkContext(conf);
/*
        Configuration hadoopconf = sc.hadoopConfiguration();
        hadoopconf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        hadoopconf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));*/
        /*hadoopconf.set("fs.defaultFS", hdfs_uri);
       // hadoopconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hadoopconf.set("dfs.permissions", "false");*/


        sc.setLogLevel("ERROR");

        SparkSession spark = SparkSession.builder()
                .appName("App")
                .getOrCreate();
        Dataset<Row> inputCity=null;



        //Get mapping city->country
        if (true) {
            switch (typeFile){
                case 0:
                    inputCity = spark.read().csv(pathListCsv[3]);
                    break;
                case 1:
                    inputCity = spark.read().format("avro").load(pathListAvro[3]);
                    break;

                case 2:

                    break;



            }

            //Dataset<Row> avroInput = spark.read().csv("data/city_attributes.csv");
            //Dataset<Row> avroInput = spark.read().format("avro").load("avro/humidity.avro");
            JavaRDD<Row> city_info = inputCity.toJavaRDD();
            Row header=city_info.first();

            //city_info.map(x->CityParser.check(x));

            //JavaRDD<String> city_info = sc.textFile(pathAvro);


            /* for avro
            JavaPairRDD<String, String> cityCountryMapRDD = avroRow.mapToPair(c -> new Tuple2<>(CityParser.parseAvro(c).getCity(), CountryMap.sendGet(c))).cache();
            JavaPairRDD<String, Float[]> cityCoordinateRDD = city_info.filter(y -> !y.equals(header)).mapToPair(c -> new Tuple2<>(CityParser.parse(c).getCity(), new Float[]{Float.parseFloat(CityParser.parse(c).getLatitude()), Float.parseFloat(CityParser.parse(c).getLongitude())}));
             */
            JavaPairRDD<String, String> cityCountryMapRDD = city_info.filter(x->CityParser.check(x,header)/*y -> !y.equals(header) && !y.anyNull()*/).mapToPair(c -> new Tuple2<>(CityParser.parse(c).getCity(), CountryMap.sendGet(c))).cache();
            JavaPairRDD<String, Float[]> cityCoordinateRDD = city_info.filter(y -> CityParser.check(y,header)).mapToPair(c -> new Tuple2<>(CityParser.parse(c).getCity(), new Float[]{Float.parseFloat(CityParser.parse(c).getLatitude()), Float.parseFloat(CityParser.parse(c).getLongitude())}));

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
                    Query1.getResponse(spark, pathListCsv[4], zoneIdList);
                    break;

                case 2:
                    /*  Process Query 2 */
                    Query2.getResponse(sc, pathListCsv, zoneIdList, hmapCities);
                    break;

                case 3:
                    /*  Process Query 3 */
                    Query3.getResponse(sc, pathListCsv[0], zoneIdList, hmapCities);
                    break;
                case 4:
                    Query1.getResponse(spark, pathListCsv[4], zoneIdList);
                    Query2.getResponse(sc, pathListCsv, zoneIdList, hmapCities);
                    Query3.getResponse(sc, pathListCsv[0], zoneIdList, hmapCities);
                default:
                    break;

            }
            sc.stop();

        }


    }
}
