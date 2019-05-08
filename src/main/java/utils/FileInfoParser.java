package utils;

import scala.Tuple2;

import java.text.ParseException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class FileInfoParser {

    private static  float MIN;
    private static  float MAX;


    public static Iterator<Tuple2<String, Float>> parseCsv(String line, String[] cities, HashMap<String,String> pairs, List<ZoneId> zoneIds,int index,boolean query3) throws ParseException {

        switch (index){
            case 0:
                MIN=191.15f;
                MAX=346.15f;
                break;
            case 1:
                MIN=955;
                MAX=1065;
                break;
            case 2:
                MIN=0;
                MAX=100;
                break;
                default:break;
        }

        ArrayList<FileInfo> fileInfoArrayList = new ArrayList<>();
        List<Tuple2<String,Float>> results=new ArrayList<>();
        FileInfo fileInfo =null;
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
                if(Float.parseFloat(csvValues[i])< MIN || Float.parseFloat(csvValues[i])> MAX )
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
                String newdate=UTCUtils.convert(zoneIds.get(i),csvValues[0]);
                String[] datetime=newdate.split("-");
                String key,country;

                if(query3){
                    country=(countries.get(i)).substring(1,countries.get(i).length()-1)+"_"+cities[i];
                    if((Integer.parseInt(datetime[2].split(" ")[1].split(":")[0])>= 12 && (Integer.parseInt(datetime[2].split(" ")[1].split(":")[0])<=15)))
                    {
                        if((datetime[1].equals("06")|| datetime[1].equals("07") || datetime[1].equals("08")|| datetime[1].equals("09")))
                          country=country+"_S";
                        else if(datetime[1].equals("01")|| datetime[1].equals("02") || datetime[1].equals("03")|| datetime[1].equals("04"))
                           country=country+"_W";
                    }
                    else
                        country=country+"_N";

                    key=datetime[0];
                }
                else {
                    key=datetime[0]+"-"+datetime[1];
                    country= (countries.get(i)).substring(1,countries.get(i).length()-1);
                }
                Tuple2<String,Float> result=new Tuple2<>(country +"_"+key,values.get(i));
                results.add(result);

            }
        }

        return results.iterator();
    }


    public static String splitAvg(String x, String y) {

        String[] avgX= x.split("_");
        String[] avgY= y.split("_");

        if(avgX[0].equals("S"))
            return String.valueOf(Double.parseDouble(avgX[1])-Double.parseDouble(avgY[1]));
        else
            return String.valueOf(Double.parseDouble(avgY[1])-Double.parseDouble(avgX[1]));
    }
}
