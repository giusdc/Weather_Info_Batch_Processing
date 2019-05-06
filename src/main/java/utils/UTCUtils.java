package utils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.iakovlev.timeshape.TimeZoneEngine;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

public class UTCUtils {

    static int count=0;
    static TimeZoneEngine engine=null;

    public static String convert(float lat,float lon,String date)throws ParseException {


        if(count==0) {
             engine= TimeZoneEngine.initialize();
        }
        Optional<ZoneId> maybeZoneId = engine.query(lat,lon);
        TimeZone timeZone=TimeZone.getTimeZone(maybeZoneId.get());
        DateTimeFormatter formatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime=LocalDateTime.parse(date,formatter);
        ZonedDateTime convertZoneDateTime=ZonedDateTime.of(localDateTime,ZoneOffset.UTC.normalized());
        ZonedDateTime newZoneDateTime = convertZoneDateTime.withZoneSameInstant(maybeZoneId.get());
        String newDate = formatter.format(newZoneDateTime);
        count++;
        return newDate;

    }

    public static String sendGet(float lat,float lon,String date) throws Exception {

        //String url2 = "https://nominatim.openstreetmap.org/search.php?q=brandenburger+tor%2C+berlin%2C+deutschland&amp;format=json";




        String url="http://api.geonames.org/timezoneJSON?lat="+lat+"&lng="+lon+"&username=giusdc";


        //45.523449,-122.676208

        //31.769039,35.216331
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");
        con.addRequestProperty("REFERER", "http://api.geonames.org");

        //add request header
        con.setRequestProperty("User-Agent", "Mozilla/5.0");

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }

        JsonObject jsonObject= (JsonObject) new JsonParser().parse(String.valueOf(response));
        //JsonObject address= (JsonObject) jsonObject.get("timezoneId");
        String country= String.valueOf(jsonObject.get("timezoneId"));




        in.close();

        //print result
        System.out.println(response.toString());
        System.out.println(country);
        return country;

    }




}
