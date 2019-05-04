package utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

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


}