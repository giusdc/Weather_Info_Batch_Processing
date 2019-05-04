import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jcp.xml.dsig.internal.dom.Utils;
import utils.WeatherInfo;
import utils.WeatherInfoParser;

import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import static org.htrace.Tracer.LOG;

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

        for(String line:weather_info.collect()){
            i++;
            if(i==2){
                System.out.println("* "+line);
                break;
            }
        }


        String header=weather_info.first();
        String[] citiesList = Arrays.copyOfRange(header.split(","), 1, header.split(",").length);

        JavaRDD<WeatherInfo> weather_infoJavaRDD=weather_info.filter(y->!y.equals(header)).
                flatMap(line->WeatherInfoParser.parseCsv(line,citiesList).iterator()).
                filter(x-> x.getDate().getMonthValue()==3 || x.getDate().getMonthValue()==4 || x.getDate().getMonthValue()==5);


        List<WeatherInfo> wi= weather_infoJavaRDD.take(15);



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
