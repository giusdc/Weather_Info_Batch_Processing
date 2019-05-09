package utils;

public class CityParser {

    public static CityInfo parseCsv(String line) {

        CityInfo city=null;
        String[] csvValues = line.split(",",-1);
        for(int i=0;i<csvValues.length;i++){
            if(csvValues[i].equals("")){
                csvValues[i]=null;
            }
        }

        //Create object
        city =new CityInfo(csvValues[0], csvValues[1], csvValues[2]);


        return city;
    }







}
