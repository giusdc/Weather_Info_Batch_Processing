package utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class CsvToAvroConverter {

    public static void converter(String[] path, String cityPath) throws Exception {
        {
            {

                String[] outputFilename = {"avro/temperature.avro","avro/pressure.avro","avro/humidity.avro","avro/cityAttributes.avro"};
                String[] schemaFilename = {"avsc/temperature.avsc","avsc/pressure.avsc","avsc/humidity.avsc","avsc/cityAttributes.avsc"};

                String[] headerCities = {"City","Latitude","Longitude"};
                String[] headerFields={"datetime","Portland","San_Francisco","Seattle","Los_Angeles","San_Diego","Las_Vegas","Phoenix","Albuquerque","Denver","San_Antonio","Dallas","Houston","Kansas_City","Minneapolis","Saint_Louis","Chicago",
                        "Nashville","Indianapolis","Atlanta","Detroit","Jacksonville","Charlotte","Miami","Pittsburgh","Philadelphia","New_York","Boston","Beersheba","Tel_Aviv_District","Eilat","Haifa","Nahariyya","Jerusalem"};

                for(int x=0;x<outputFilename.length;x++) {
                    File schemaFile = new File(schemaFilename[x]);
                    CsvToAvroGenericWriter writer = new CsvToAvroGenericWriter(schemaFile,outputFilename[x],CsvToAvroGenericWriter.MODE_WRITE);
                    if(x==3)
                        writer.setCsvHeader(headerCities);
                    else
                        writer.setCsvHeader(headerFields);
                    int i=0;
                    try (BufferedReader br = new BufferedReader(new FileReader(path[x]))) {
                        String line;

                        while ((line = br.readLine()) != null) {

                            i++;
                            if(i!=1)
                                writer.append(line);


                        }
                    }
                    writer.closeWriter();
                }





                File outputFile = new File(outputFilename[2]);
                DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(outputFile, new GenericDatumReader<GenericRecord>());
                while (reader.hasNext()) {
                    System.out.println(reader.next().toString());
                    /*
                    JsonObject jsonObject= (JsonObject) new JsonParser().parse(reader.next().toString());
                    String country= String.valueOf(jsonObject.get("City"));*/
                    //System.out.println(country);
                }
                reader.close();

            }
        }
    }
}

