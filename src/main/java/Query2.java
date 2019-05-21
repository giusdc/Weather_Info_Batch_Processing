import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import utils.*;

import java.io.IOException;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;

public class Query2 {


    public static JavaRDD<Row> getResponse(SparkSession spark, String[] pathList, List<ZoneId> zoneIdList, HashMap<String,String> hmapCities, String format,String[] citiesList) throws ParseException, IOException {

        JavaRDD<Row> filterRDD,prova=null;
        String cities[]=null;

        //Get temperature values
        for(int i=0;i<pathList.length-2;i++){
            int index=i;
            Dataset<Row> fileRow = spark.read().format(format).load(pathList[i]);
            JavaRDD<Row> fileRDD = fileRow.toJavaRDD();

            filterRDD = fileRDD.filter(x->FileInfoParser.check(x)).cache();

            if(index==0){
                 prova = fileRDD.filter(x -> FileInfoParser.check(x)).cache();
                //tempRDD=filterRDD;
            }

            JavaPairRDD<String, Float> fileInfoRDD = filterRDD.flatMapToPair(line -> FileInfoParser.parse(line, citiesList, hmapCities, zoneIdList, index, false)).sortByKey().cache();

            //tempInfoRDD.saveAsTextFile("ciao");

            JavaPairRDD<String, Stats> avgRDD = fileInfoRDD.aggregateByKey((new Stats(0,0,0,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY)),
                    (v,x) -> new Stats(v.getSum()+x,v.getNum()+1,v.getSumSquare()+Math.pow(x,2),v.computeMin(x),v.computeMax(x)),
                    (v1,v2) -> new Stats(v1.getSum()+v2.getSum(),v1.getNum()+v2.getNum(),v1.getSumSquare()+v2.getSumSquare(),Math.min(v1.getMin(),v2.getMin()),Math.max(v1.getMax(),v2.getMax())));

            JavaPairRDD<String,String> AvgResult = avgRDD.mapToPair( p->new Tuple2<>(p._1(), p._2().getValues())).sortByKey();
            //TODO
            AvgResult.saveAsTextFile(Main.hdfs_uri+"/user/query2/"+i);



            long endTime = System.currentTimeMillis();
            long timeElapsed = endTime - Main.startTime;
            System.out.println("Execution time in seconds: " + timeElapsed / 1000);


            HBaseUtils.execute("/user/query2/"+i+"/part-00000",2,i,Main.hdfs_uri);


        }
        FileInfoParser.Result result=new FileInfoParser.Result(prova,cities);
        return prova;
    }

}
