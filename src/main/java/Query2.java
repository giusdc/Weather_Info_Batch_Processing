import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.*;

import java.text.ParseException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query2 {


    private static String pathToCityFile = "data/city_attributes.csv";
    private static String temperaturePath= "data/temperature.csv";
    private static String humidityPath= "data/humidity.csv";
    private static String pressurePath= "data/pressure.csv";


    public static void getResponse(JavaSparkContext sc,String[] pathList,List<ZoneId> zoneIdList,HashMap<String,String> hmapCities) throws ParseException {


        //Get temperature values
        for(int i=0;i<pathList.length;i++){
            int index=i;
            JavaRDD<String> tempInfo= sc.textFile(pathList[i]);
            String headerTemp=tempInfo.first();
            String[] citiesList = Arrays.copyOfRange(headerTemp.split(","), 1, headerTemp.split(",").length);
            JavaPairRDD<String,Float> tempInfoRDD=tempInfo.filter(y->!y.equals(headerTemp))
                    .flatMapToPair(line-> TempInfoParser.parseCsv(line,citiesList,hmapCities,zoneIdList,index)).sortByKey().cache();


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
            outputTemp.saveAsTextFile("output_query2/"+pathList[i]);
        }


        sc.stop();
    }

}
