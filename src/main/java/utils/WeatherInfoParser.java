package utils;

import java.util.ArrayList;
import java.util.List;

public class WeatherInfoParser {




    public static ArrayList<WeatherInfo> parseCsv(String line,String[] cities) {


        ArrayList<WeatherInfo> weatherInfoArrayList= new ArrayList<>();
        WeatherInfo weatherInfo=null;



        String[] csvValues = line.split(",");
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
}
