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
        JavaPairRDD<String,Float> tempInfoRDD=tempInfo.filter(y->!y.equals(headerTemp))
                .flatMapToPair(line-> TempInfoParser.parseCsv(line,citiesList,hmapCities)).sortByKey().cache();


        JavaPairRDD<String,Avg> avgRDD = tempInfoRDD.aggregateByKey(new Avg(0,0),
                (v,x) -> new Avg(v.getSum()+x,v.getNum()+1),
                (v1,v2) -> new Avg(v1.getSum()+v2.getSum(),v1.getNum()+v2.getNum()));

        JavaPairRDD<String,Double> AvgResult = avgRDD.mapToPair( p->new Tuple2<>(p._1(), p._2().getAvg())).sortByKey();

        Map<String,Double> m =  AvgResult.collectAsMap();
        HashMap<String,Double> mapAVG = new HashMap<String,Double>(m);

        JavaPairRDD<String,Double> pair= tempInfoRDD.mapToPair(l-> new Tuple2<>(l._1(),Math.pow(l._2()-mapAVG.get(l._1()),2)));
        JavaPairRDD<String,Var> varRDD = pair.aggregateByKey(new Var(0,0),

                (v,x) -> new Var(v.getValue()+x,v.getN()+1),
                (v1,v2) -> new Var(v1.getValue()+v2.getValue(),v1.getN()+v2.getN()));
        JavaPairRDD<String,Double> varResult = varRDD.mapToPair( p->new Tuple2<>(p._1(),  p._2().getVar())).sortByKey();

        JavaPairRDD<String,Float> minRDD = tempInfoRDD.reduceByKey((a, b) -> Math.min(a,b)).sortByKey();

        JavaPairRDD<String,Float> maxRDD = tempInfoRDD.reduceByKey((a, b) -> Math.max(a,b)).sortByKey();

        JavaPairRDD<String, Tuple2<Tuple2<Tuple2<Double, Double>, Float>, Float>> outputTemp = AvgResult.join(varResult).join(minRDD).join(maxRDD).sortByKey();
        //JavaPairRDD<String, Float> outputTemp = AvgResult.union(varResult, minRDD, maxRDD);
        outputTemp.saveAsTextFile("output_query2");
        sc.stop();
    }


}
