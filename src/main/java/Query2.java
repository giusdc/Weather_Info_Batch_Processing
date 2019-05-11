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

public class Query2 {


    public static void getResponse(JavaSparkContext sc,String[] pathList,List<ZoneId> zoneIdList,HashMap<String,String> hmapCities) throws ParseException {


        //Get temperature values
        for(int i=0;i<pathList.length-1;i++){
            int index=i;
            JavaRDD<String> tempInfo= sc.textFile(pathList[i]);
            String headerTemp=tempInfo.first();
            String[] citiesList = Arrays.copyOfRange(headerTemp.split(","), 1, headerTemp.split(",").length);

            JavaPairRDD<String,Float> tempInfoRDD=tempInfo.filter(y->!y.equals(headerTemp))
                    .flatMapToPair(line-> FileInfoParser.parseCsv(line,citiesList,hmapCities,zoneIdList,index,false)).sortByKey().cache();

            //tempInfoRDD.saveAsTextFile("ciao");

            JavaPairRDD<String, Stats> avgRDD = tempInfoRDD.aggregateByKey((new Stats(0,0,0,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY)),
                    (v,x) -> new Stats(v.getSum()+x,v.getNum()+1,v.getSumSquare()+Math.pow(x,2),v.computeMin(x),v.computeMax(x)),
                    (v1,v2) -> new Stats(v1.getSum()+v2.getSum(),v1.getNum()+v2.getNum(),v1.getSumSquare()+v2.getSumSquare(),Math.min(v1.getMin(),v2.getMin()),Math.max(v1.getMax(),v2.getMax())));

            JavaPairRDD<String,String> AvgResult = avgRDD.mapToPair( p->new Tuple2<>(p._1(), p._2().getValues())).sortByKey();
            AvgResult.saveAsTextFile("output_query2/"+pathList[i]);
        }

    }

}
