
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import utils.WeatherInfo;
import utils.WeatherInfoParser;

import java.text.ParseException;

import java.util.Arrays;

public class Query1 {

    private static String pathToFile = "data/weather_description.csv";
    public static void main(String[] args) throws ParseException {
        
        int i=0;

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query1");
        JavaSparkContext sc = new JavaSparkContext(conf);



/*
        SparkSession spark = SparkSession
                .builder()
                .appName("csv2parquet").config("spark.sql.warehouse.dir", "/file:/tmp")
                .master("local")
                .getOrCreate();

        //JavaRDD<String> weatherFile = sc.textFile(pathToFile);

        Dataset<Row> ds = spark.read().option("inferSchema", true).csv(pathToFile);

        final String parquetFile = "test.parquet";
        final String codec = "csv";

        //ds.write().save(parquetFile);

        //ds.write().format("com.databricks.spark.csv").save("pippo.csv");
        //ds.write().save("output.prova");

        //spark.stop();

        ds.javaRDD().saveAsTextFile("data/pippo.parquet");

        //Dataset<Row> pippo=spark.read().parquet(parquetFile);*/


        JavaRDD<String> weather_info= sc.textFile("data/weather_description.csv");




        String header=weather_info.first();
        String[] citiesList = Arrays.copyOfRange(header.split(","), 1, header.split(",").length);

        JavaRDD<WeatherInfo> weather_infoJavaRDD=weather_info.filter(y->!y.equals(header)).
                flatMap(line->WeatherInfoParser.parseCsv(line,citiesList).iterator()).
                filter(x-> x.getDate().getMonthValue()==3 || x.getDate().getMonthValue()==4 || x.getDate().getMonthValue()==5);

        JavaPairRDD<String,Integer> weatherPair=weather_infoJavaRDD.mapToPair(w->new Tuple2<>(WeatherInfoParser.getCityAndDay(w),WeatherInfoParser.getDescription(w)))
                .reduceByKey((k,z)->k+z)
                .filter(f->f._2()>18)
                .mapToPair(p->new Tuple2<>(WeatherInfoParser.getKey(p._1()),1))
                .reduceByKey((x,y)->x+y).filter(p->p._2()>=15);

        weatherPair.saveAsTextFile("output");








       /* for(Tuple2<String,Integer> pippo:count.collect()){
            System.out.println(pippo._1+" "+pippo._2);
        }*/


        for(String line:weather_info.collect()){
            i++;
            if(i==2){
                System.out.println("* "+line);
                break;
            }
        }



        sc.stop();
    }






}
