package utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;

public class CityParser {

    public static CityInfo parse(Row line) throws IOException {

        CityInfo city=null;

        String[] values=new String[line.length()];


        //String[] csvValues = line.split(",",-1);
        for(int i=0;i<line.length();i++){
            if(line.get(i).equals("")){
                values[i]=null;

            }else{
                values[i]=line.get(i).toString();
            }
        }


        //Create object
        city =new CityInfo(values[0], values[1], values[2]);


        return city;
    }

    /*

    public static CityInfo parseAvro(Row row) {

        CityInfo city=null;

        for(int i=0; i<row.length();i++){
            String[] values = .split(",",-1);

        }


        //Create object
        city =new CityInfo(values[0], values[1], values[2]);


        return city;
    }



     */

}
