import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.FileInfoParser;
import utils.Stats;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Query3 {
    public static void getResponse(JavaSparkContext sc, String pathFile, List<ZoneId> zoneIdList, HashMap<String,String> hmapCities) {

        //Get temperature values
        JavaRDD<String> tempInfo= sc.textFile(pathFile);
        String headerTemp=tempInfo.first();
        String[] citiesList = Arrays.copyOfRange(headerTemp.split(","), 1, headerTemp.split(",").length);

        JavaPairRDD<String,Float> tempInfoRDD=tempInfo.filter(y->!y.equals(headerTemp))
                .flatMapToPair(line-> FileInfoParser.parseCsv(line,citiesList,hmapCities,zoneIdList,0,true))
                .filter(x->(x._1().split("_")[2].equals("S") || x._1().split("_")[2].equals("W")) &&
                                (x._1().split("_")[3].split("-")[0].equals("2017") ||  x._1().split("_")[3].split("-")[0].equals("2016")));

        JavaPairRDD<String, Stats> avgRDD = tempInfoRDD.aggregateByKey((new Stats(0,0)),
                (v,x) -> new Stats(v.getSum()+x,v.getNum()+1),
                (v1,v2) -> new Stats(v1.getSum()+v2.getSum(),v1.getNum()+v2.getNum()));

        JavaPairRDD<String,String> avgResult = avgRDD.mapToPair( p->new Tuple2<>(p._1().split("_")[0]+"_"+p._1().split("_")[1]+"_"+p._1().split("_")[3] , p._1().split("_")[2]+"_"+p._2().getAvg()));
        JavaPairRDD<String, String> diffAvg = avgResult.reduceByKey((x, y) -> FileInfoParser.splitAvg(x, y))
                                                        .mapToPair(p->new Tuple2<>(p._1().split("_")[0]+"_"+p._1().split("_")[2],p._2()+"_"+p._1().split("_")[1]));

        JavaPairRDD<String, Stats> rankRDD = diffAvg.aggregateByKey((new Stats(null)),
                (v,x) -> new Stats(v.addElement(x)),
                (v1,v2) -> new Stats(Lists.newArrayList(Iterables.concat(v1.getRank(),v2.getRank()))));
        //
        JavaPairRDD<String,String>output=rankRDD.mapToPair(p->new Tuple2<>(p._1(),p._2().computeRank()));

        //diffAvg.saveAsTextFile("ciao");
        output.saveAsTextFile("output_query3");

    }
}
