package utils;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TimeUtils {
    private static ArrayList<Long> citytime=new ArrayList<Long>();
    private static ArrayList<Long> query1time=new ArrayList<Long>();
    private static ArrayList<Long> query2time=new ArrayList<Long>();
    private static ArrayList<Long> query3time=new ArrayList<Long>();
    private static ArrayList<Long> query2sql=new ArrayList<Long>();
    private static ArrayList<Long> finaltime=new ArrayList<Long>();
    private static ArrayList<Float> mean=new ArrayList<Float>();

    public static ArrayList<Long> getCitytime() {
        return citytime;
    }

    public static ArrayList<Long> getQuery1time() {
        return query1time;
    }

    public static ArrayList<Long> getQuery2time() {
        return query2time;
    }

    public static ArrayList<Long> getQuery3time() {
        return query3time;
    }

    public static ArrayList<Long> getQuery2sql() {
        return query2sql;
    }

    public static ArrayList<Long> getFinaltime() {
        return finaltime;
    }

    public static void calculateTime(long startTime, long endTime, int index){
        long diff=(endTime - startTime)/1000;
        switch (index){
            case 0:
                citytime.add(diff);
                break;
            case 1:
                query1time.add(diff);
                break;
            case 2:
                query2time.add(diff);
                break;
            case 3:
                query3time.add(diff);
                break;
            case 4:
                query2sql.add(diff);
                break;
            case 5:
                finaltime.add(diff);
                break;
            default:
                break;
        }

    }

    private static float mean(List<Long> list){
        long sum = 0;
        for(long time: list ){
            sum+=time;
        }
        float temp=(float) sum/list.size();
        mean.add(temp);
        return temp;
    }

    public static void compute(FSDataOutputStream writer) throws IOException {
        writer.writeBytes("Query city mean time(s): "+mean(citytime)+"\n");
        writer.writeBytes("Query 1 mean time(s): "+mean(query1time)+"\n");
        writer.writeBytes("Query 2 mean time(s): "+mean(query2time)+"\n");
        writer.writeBytes("Query 3 mean time(s): "+mean(query3time)+"\n");
        writer.writeBytes("Query final mean time(s): "+mean(finaltime)+"\n");
        writer.writeBytes("Query 2 sql mean time(s): "+mean(query2sql)+"\n");
        writer.writeBytes("******************\n");
        writer.writeBytes("Query city dev time(s): "+dev(0,citytime)+"\n");
        writer.writeBytes("Query 1 dev time(s): "+dev(1,query1time)+"\n");
        writer.writeBytes("Query 2 dev time(s): "+dev(2,query2time)+"\n");
        writer.writeBytes("Query 3 dev time(s): "+dev(3,query3time)+"\n");
        writer.writeBytes("Query final dev time(s): "+dev(4,finaltime)+"\n");
        writer.writeBytes("Query 2 sql dev time(s): "+dev(5,query2sql)+"\n");

    }

    private static float dev(int i, ArrayList<Long> arrayList) {
        double sum=0;
        for(long x:arrayList){
             sum+= Math.pow(x - mean.get(i), 2);
        }
        return (float) Math.sqrt(sum/arrayList.size());
    }
}
