package utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import scala.collection.Map;
import scala.collection.Seq;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class CityParser {

    public static CityInfo parse(Row line) throws IOException {

        return new CityInfo(line.get(0).toString(), line.get(1).toString(), line.get(2).toString());
    }

    public static boolean check(Row x, Row header) {

        if (!x.anyNull())
            return !x.anyNull() && !x.equals(header) && !(x.get(0).toString().matches(".*\\d.*")) && check_coordinate(x.get(1).toString(), x.get(2).toString());
        return false;


    }

    private static boolean check_coordinate(String lat, String lon) {
        float lat_value, lon_value;
        try {
            lat_value = Float.parseFloat(lat);
            lon_value = Float.parseFloat(lon);
            if ((lat_value >= -90 && lat_value <= 90) && (lon_value >= -180 && lon_value <= 180)) {
                return true;
            }
        } catch (NumberFormatException nfe) {
            return false;
        }
        return false;
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
