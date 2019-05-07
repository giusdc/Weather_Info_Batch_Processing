package utils;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class TempInfoParser {

    private static final float MIN_TEMP = 191.15f;
    private static final float MAX_TEMP = 346.15f;


    public static Iterator<Tuple2<String, Float>> parseCsv(String line, String[] cities, HashMap<String,String> pairs) {


        ArrayList<TempInfo> tempInfoArrayList= new ArrayList<>();
        List<Tuple2<String,Float>> results=new ArrayList<>();
        TempInfo tempInfo=null;
        String[] csvValues = line.split(",",-1);
        for(int i=0;i<csvValues.length;i++){
            if(csvValues[i].equals("")){
                csvValues[i]=null;
            }
        }


        //Extract descriptions
        ArrayList<Float> values=new ArrayList<>();
        for(int i=1;i<(csvValues.length);i++){
            if(csvValues[i]!=null){
                if(Float.parseFloat(csvValues[i])< MIN_TEMP || Float.parseFloat(csvValues[i])> MAX_TEMP )
                    values.add(null);
                else
                    values.add(Float.parseFloat(csvValues[i]));

            }
            else
                values.add(null);
        }

        ArrayList<String> countries=new ArrayList<>();

        for(int i=0;i<cities.length;i++){

            countries.add(pairs.get(cities[i]));
        }




        //Create object

        for(int i=0;i<cities.length;i++){
            if(values.get(i)!=null){
                /*tempInfo=new TempInfo(
                        countries.get(i),
                        csvValues[0],
                        values.get(i));*/
                String[] datetime=csvValues[0].split("-");
                String key=datetime[0]+"-"+datetime[1];
                String country= (countries.get(i)).substring(1,countries.get(i).length()-1);
                Tuple2<String,Float> result=new Tuple2<>(country +"_"+key,values.get(i));
                results.add(result);

                //tempInfoArrayList.add(tempInfo);
            }
        }

        return results.iterator();
    }


}
