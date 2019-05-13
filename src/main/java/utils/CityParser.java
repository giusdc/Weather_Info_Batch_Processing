package utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.IOException;

public class CityParser {

    public static CityInfo parseCsv(String line) throws IOException {

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
