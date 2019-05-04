package utils;

import java.util.ArrayList;
import java.util.StringTokenizer;

public class CityParser {

    public static City parseCsv(String line) {

        City city=null;
        String[] csvValues = line.split(",",-1);
        for(int i=0;i<csvValues.length;i++){
            if(csvValues[i].equals("")){
                csvValues[i]=null;
            }
        }

        //Create object
        city =new City(csvValues[0], csvValues[1], csvValues[2]);


        return city;
    }







}
