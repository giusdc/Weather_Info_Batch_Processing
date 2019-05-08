package utils;

import net.iakovlev.timeshape.TimeZoneEngine;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class WeatherInfoParser {




    public static ArrayList<WeatherInfo> parseCsv(String line,String[] cities) {


        ArrayList<WeatherInfo> weatherInfoArrayList= new ArrayList<>();
        WeatherInfo weatherInfo=null;


        StringTokenizer tokenizer = new StringTokenizer(line, ",");
        ArrayList<String> arr=new ArrayList<>();

        while (tokenizer.hasMoreTokens()) {
            arr.add(tokenizer.nextToken());

        }





        String[] csvValues = line.split(",",-1);
        for(int i=0;i<csvValues.length;i++){
            if(csvValues[i].equals("")){
                csvValues[i]=null;
            }
        }


        //Extract descriptions
        ArrayList<String> descriptions=new ArrayList<>();
        for(int i=1;i<(csvValues.length);i++){
            descriptions.add(csvValues[i]);
        }


        //Create object

        for(int i=0;i<cities.length;i++){
            weatherInfo=new WeatherInfo(
                    csvValues[0],
                    descriptions.get(i),
                    cities[i]

            );
            weatherInfoArrayList.add(weatherInfo);
        }

        return weatherInfoArrayList;
    }



    public static Iterator<Tuple2<String, Integer>> parseCsv2(String line, String[] cities, List<ZoneId> zoneIdList) throws ParseException {

        ArrayList<WeatherInfo> weatherInfoArrayList= new ArrayList<>();
        List<Tuple2<String,Integer>> results=new ArrayList<>();
        WeatherInfo weatherInfo=null;
        String[] csvValues = line.split(",",-1);
        for(int i=0;i<csvValues.length;i++){
            if(csvValues[i].equals("")){
                csvValues[i]=null;
            }
        }

        //Extract descriptions
        ArrayList<String> descriptions=new ArrayList<>();
        for(int i=1;i<(csvValues.length);i++){
            if(csvValues[i]!=null)
                descriptions.add(csvValues[i]);
            else
                descriptions.add(null);
        }

        //Create object
        int x=0;
        for(int i=0;i<cities.length;i++){
            if(descriptions.get(i)!=null){
                String newdate=UTCUtils.convert(zoneIdList.get(i),csvValues[0]);
                String[] datetime=newdate.split("-");
                String date=datetime[0]+"-"+datetime[1]+"-"+datetime[2].split(" ")[0];
                if(descriptions.get(i).equals("sky is clear"))
                    x=1;
                else x=0;
                Tuple2<String,Integer> result=new Tuple2<>(cities[i]+"_"+date,x);
                results.add(result);
            }
        }
        return results.iterator();
    }


    public static String getCityAndDay(WeatherInfo wi) throws ParseException {

        LocalDate date=wi.getDate();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedString = (String) date.format(formatter);
        return wi.getCity()+"_"+formattedString;
    }

    public static int getDescription(WeatherInfo wi){
        if(wi.getDescription().equals("sky is clear")){
            return 1;
        }
        return 0;
    }

    public static String getKey(String value){
        String[] stringValue = value.split("_");
        String[] datetime=stringValue[1].split("-");
        String key=stringValue[0]+"_"+datetime[0]+"-"+datetime[1];
        return key;
    }

    public static String getKey2(String value){
        String[] stringValue = value.split("_");
        String[] datetime=stringValue[1].split("-");
        String key=stringValue[0]+"_"+datetime[0];
        return key;
    }


}
