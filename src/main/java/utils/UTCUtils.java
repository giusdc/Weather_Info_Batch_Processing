package utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.iakovlev.timeshape.TimeZoneEngine;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class UTCUtils {

    static int count=0;
    static TimeZoneEngine engine=null;

    public static String convert(ZoneId zoneId,String date)throws ParseException {



        DateTimeFormatter formatter=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime=LocalDateTime.parse(date,formatter);
        ZonedDateTime convertZoneDateTime=ZonedDateTime.of(localDateTime,ZoneOffset.UTC.normalized());
        ZonedDateTime newZoneDateTime = convertZoneDateTime.withZoneSameInstant(zoneId);
        String newDate = formatter.format(newZoneDateTime);
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


    public static List<Tuple2<String, ZoneId>> getZoneId(List<Float[]> latlon, String[] cities)throws ParseException {


        TimeZoneEngine engine= TimeZoneEngine.initialize();
        List<Tuple2<String,ZoneId>> results=new ArrayList<>();
        int i=0;
        for(Float[] value:latlon) {
            Optional<ZoneId> maybeZoneId = engine.query(value[0], value[1]);
            ZoneId zoneId = maybeZoneId.get();
            Tuple2<String,ZoneId> result=new Tuple2<>(cities[i],zoneId);
            i++;
            results.add(result);
        }
        return results;

    }

    public static void read() throws IOException {

        System.out.println("pippo");
        String hdfsuri = "hdfs://3.122.52.163:8020";

        String path="/user/prova/";
        String fileName="part-00000";
        //String fileContent="hello;world";

        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsuri);
        //conf.set("fs.default.name", conf.get("fs.defaultFS"));
        //conf.set("dfs.nameservices","yourclustername");
        //conf.set("dfs.ha.namenodes.yourclustername", "nn1,nn2");


        //conf.set("dfs.client.failover.proxy.provider.yourclustername","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        // Set FileSystem URI
        //conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        //conf.set("dfs.client.use.datanode.hostname", "true");
        //conf.set("dfs.datanode.address","52.47.161.215:50010");
        //conf.addResource("hdfs-site.xml");

      //  conf.set("dfs.datanode.use.datanode.hostname","true");
        //conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
        //conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");

        //Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsuri),conf);
        Path workingDir=fs.getWorkingDirectory();
        Path newFolderPath= new Path(path);
       if(!fs.exists(newFolderPath)) {
            // Create new Directory
            fs.mkdirs(newFolderPath);
            System.out.println("Path "+path+" created.");
        }

/*
        //==== Write file
        System.out.println("Begin Write file into hdfs");
        //Create a path
        Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
        //Init output stream
        FSDataOutputStream outputStream=fs.create(hdfswritepath);
        //Cassical output stream usage
        outputStream.writeBytes(fileContent);
        outputStream.close();
        System.out.println("End Write file into hdfs");*/

        Path hdfsreadpath = new Path("/user/prova/part-00000");
        //Init input stream
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        //Classical input stream usage
        String out= IOUtils.toString(inputStream, "UTF-8");
        System.out.println(out);
        inputStream.close();
        fs.close();

    }


}
