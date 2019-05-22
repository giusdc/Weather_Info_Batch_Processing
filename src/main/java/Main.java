import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
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
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static String hdfs_uri = "hdfs://35.157.71.200:8020";

    //private static String[] pathListCsv = {"data/temperature.csv", "data/pressure.csv", "data/humidity.csv", "data/city_attributes.csv", "data/weather_description.csv"};
    //private static String[] pathListAvro = {"avro/temperature.avro", "avro/pressure.avro", "avro/humidity.avro", "avro/cityAttributes.avro", "avro/weather_description.avro"};

    private static String[] pathListCsv = {hdfs_uri + "/user/hdfs/temperature.csv", hdfs_uri + "/user/hdfs/pressure.csv", hdfs_uri + "/user/hdfs/humidity.csv", hdfs_uri + "/user/hdfs/city_attributes.csv", hdfs_uri + "/user/hdfs/weather_description.csv"};
    private static String[] pathListAvro = {hdfs_uri + "/user/hdfs/temperature.avro", hdfs_uri + "/user/hdfs/pressure.avro", hdfs_uri + "/user/hdfs/humidity.avro", hdfs_uri + "/user/hdfs/city_attributes.avro", hdfs_uri + "/user/hdfs/weather_description.avro"};
    private static String[] pathListParquet = {hdfs_uri + "/user/hdfs/temperature.parquet", hdfs_uri + "/user/hdfs/pressure.parquet", hdfs_uri + "/user/hdfs/humidity.parquet", hdfs_uri + "/user/hdfs/city_attributes.parquet", hdfs_uri + "/user/hdfs/weather_description.parquet"};

    //private static String pathAvro="avro/cityAttributes.avro";
    public static long startTime;
    public static int typeFile; //0 for csv,1 for avro,2 for parquet


    public static void main(String[] args) throws Exception {

        int query = Integer.parseInt(args[0]);
        typeFile = Integer.parseInt(args[1]);

        //CsvToAvroConverter.converter(pathListCsv, pathToCityFile);

        //UTCUtils.read();
        //HDFSUtils.init(pathListCsv);

        //starting timer
        startTime = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                //.setMaster("local")
                .setAppName("Query");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration hadoopconf = sc.hadoopConfiguration();
        //hadoopconf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        //hadoopconf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        hadoopconf.set("fs.defaultFS", hdfs_uri);
        hadoopconf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopconf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        hadoopconf.set("dfs.permissions", "false");


        sc.setLogLevel("ERROR");

        SparkSession spark = SparkSession.builder()
                .appName("App")
                .getOrCreate();
        Dataset<Row> inputCity=null;



        //Get mapping city->country
        if (true) {
            String[] path=null;
            String format="";
            switch (typeFile){
                case 0:
                    inputCity = spark.read().format("csv").load(pathListCsv[3]);
                    format="csv";
                    path= pathListCsv;
                    break;
                case 1:
                    DatumReader<GenericRecord> datumReader= new GenericDatumReader<>();
                    Path path1= new Path(pathListAvro[3]);
                    SeekableInput input= new FsInput(path1,hadoopconf);
                    DataFileReader<GenericRecord> dataFileReader= new DataFileReader<GenericRecord>(input,datumReader);
                    System.out.println("schema"+dataFileReader.getSchema().getFields().get(0));
                    inputCity = spark.read().format("avro").load(pathListAvro[3]);
                    format="avro";
                    path = pathListAvro;
                    break;

                case 2:
                    inputCity = spark.read().format("parquet").load(pathListParquet[3]);
                    format="parquet";
                    path = pathListParquet;
                    break;
            }

            JavaRDD<Row> city_info = inputCity.toJavaRDD();

            Row header=city_info.first();
            JavaPairRDD<String, String> cityCountryMapRDD = city_info.filter(x->CityParser.check(x)).mapToPair(c -> new Tuple2<>(CityParser.parse(c).getCity(), CountryMap.sendGet(c))).cache();
            JavaPairRDD<String, Float[]> cityCoordinateRDD = city_info.filter(y -> CityParser.check(y)).mapToPair(c -> new Tuple2<>(CityParser.parse(c).getCity(), new Float[]{Float.parseFloat(CityParser.parse(c).getLatitude()), Float.parseFloat(CityParser.parse(c).getLongitude())}));

            Map<String, String> map = cityCountryMapRDD.collectAsMap();
            HashMap<String, String> hmapCities = new HashMap<String, String>(map);
            List<Float[]> values = cityCoordinateRDD.values().collect();
            List<String> cityList = cityCoordinateRDD.keys().collect();
            String[] citiesName = cityList.toArray(new String[cityList.size()]);


            //Get mapping pair zoneId/cities
            List<Tuple2<String, ZoneId>> mapping = UTCUtils.getZoneId(values, citiesName);
            JavaRDD rdd = sc.parallelize(mapping);
            JavaPairRDD<String, ZoneId> mappingPair = JavaPairRDD.fromJavaRDD(rdd);
            // mappingPair.saveAsTextFile("hdfs://3.122.52.163:8020/user/prova");
            List<ZoneId> zoneIdList = mappingPair.values().collect();

            FileInfoParser.Result result=null;
            JavaRDD<Row> row=null;


            switch (query) {
                case 1:
                    /*  Process Query 1 */
                    Query1.getResponse(spark, path[4], zoneIdList,format,citiesName);
                    break;

                case 2:
                    /*  Process Query 2 */
                    Query2.getResponse(spark, path, zoneIdList, hmapCities, format,citiesName);
                    break;

                case 3:
                    /*  Process Query 3 */
                    Query3.getResponse(spark, path[0], zoneIdList, hmapCities,format,citiesName);
                    break;
                case 4:
                    Query1.getResponse(spark, path[4], zoneIdList,format,citiesName);
                    row=Query2.getResponse(spark, path, zoneIdList, hmapCities,format,citiesName);
                    Query3.getResponse(spark, path[0], zoneIdList, hmapCities,format,citiesName);
                    //Query3.getResponse(row,zoneIdList,hmapCities);
                default:
                    break;

            }
            spark.stop();
            sc.stop();

        }


    }
}
