package utils;

import com.google.protobuf.ServiceException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;


public class HBaseUtils {
    private static final String ZOOKEEPER_HOST = "localhost";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String HBASE_MASTER  = "localhost:60000";
    private static final int    HBASE_MAX_VERSIONS = Integer.MAX_VALUE;

    private static byte[] b(String s){
        return Bytes.toBytes(s);
    }


    public static void main(String[] args) throws IOException, ServiceException, org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException {



        HBaseClient hbc = new HBaseClient();

        int choice = 1;

        switch (choice) {

            case 1:

                /* **********************************************************
                 *  Table Management: Create, Alter, Describe, Delete
                 * ********************************************************** */
                tableManagementOperations(hbc);
                break;


        }

    }

    public static void tableManagementOperations(HBaseClient hbc) throws IOException, ServiceException, org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException {

        /* **********************************************************
         *  Table Management: Create, Alter, Describe, Delete
         * ********************************************************** */
        //  Create

        System.out.println("\n ------------------\n");
        System.out.println("Adding value: row1:fam1:col1=val1");
        hbc.put("query1", "2017", "fam1", "lista", "Pippo,Pluto");
        System.out.println("Adding value: row1:fam1:col2=val2");
        hbc.put("query1", "2018", "fam1", "lista", "ciao,ciao");


        /* Get */
        String v1 = hbc.get("query1",
                "2017", "fam1", "lista");
        String v2 = hbc.get("query1",
                "2018", "fam1", "lista");
        System.out.println("Retrieving values : " + v1 + "; " + v2);
        System.out.println("\n ------------------\n");

    }


    public static void execute(String pathHDFS,int query, int idFile,String hdfsuri) throws IOException {

        HBaseClient hbc = new HBaseClient();
        createTable(hbc,query);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsuri);

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");

        //Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsuri),conf);

        Path hdfsreadpath = new Path(pathHDFS);
        //Init input stream
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String st;
        System.out.println("query"+query);
        while((st=reader.readLine())!=null){
            putData(hbc,st,query,idFile);
        }



    }

    private static void delete(HBaseClient hbc,int query) {
        switch (query){
            case 1:
                if (hbc.exists("query1")){
                    System.out.println("Delete table...");
                    hbc.dropTable("query1");
                }
            case 2:
                if (hbc.exists("query2")){
                    System.out.println("Delete table...");
                    hbc.dropTable("query2");
                }
            case 3:
                if (hbc.exists("query3")){
                    System.out.println("Delete table...");
                    hbc.dropTable("query3");
                }

        }

    }


    private static String getFamilyById(int index){

        String result=null;
        switch (index){
            case 0:
                result="temp";
                break;
            case 1:
                result="press";
                break;

            case 2:
                result="hum";
                break;

                default:
                    break;
        }
        return result;

    }

    private static void putData(HBaseClient hbc,String line,int query,int idFile) {

        String[] result;
        String v1;
        System.out.println("putq"+query);
        switch (query){
            case 1:
                result=line.split(",",2);

                hbc.put("query1", result[0].substring(1), "fam1", "city_list", result[1].substring(0,result[1].length()-1));
                break;

            case 2:
                String familyName=getFamilyById(idFile);
                result=line.split(",",2);
                String[]value=result[1].split(",");

                hbc.put("query2", result[0].substring(1), familyName, "mean",value[0]);
                hbc.put("query2", result[0].substring(1), familyName, "std dev",value[1]);
                hbc.put("query2", result[0].substring(1), familyName, "min",value[2]);
                hbc.put("query2", result[0].substring(1), familyName, "max",value[3].substring(0,value[3].length()-1));
                break;


            case 3:
                result=line.split(",",2);
                hbc.put("query3", result[0].substring(1), "pos1", "city", result[1].split(",")[0].split("_")[0]);
                hbc.put("query3", result[0].substring(1), "pos1", "pos2016", result[1].split(",")[0].split("_")[1]);
                hbc.put("query3", result[0].substring(1), "pos2", "city", result[1].split(",")[1].split("_")[0]);
                hbc.put("query3", result[0].substring(1), "pos2", "pos2016", result[1].split(",")[1].split("_")[1]);
                hbc.put("query3", result[0].substring(1), "pos3", "city", result[1].split(",")[2].split("_")[0]);
                hbc.put("query3", result[0].substring(1), "pos3", "pos2016", result[1].split(",")[2].split("_")[1]);
                break;

        }


    }

    private static void createTable(HBaseClient hbc,int query) {
        switch (query){
            case 1:
                if (!hbc.exists("query1")){
                    System.out.println("Creating table...");
                    hbc.createTable("query1","fam1");
                }
                break;
            case 2:
                if (!hbc.exists("query2")){
                    System.out.println("Creating table...");
                    hbc.createTable("query2","temp","press","hum");
                }
                break;
            case 3:
                if (!hbc.exists("query3")){
                    System.out.println("Creating table...");
                    hbc.createTable("query3","pos1","pos2","pos3");
                }
                break;
                default:
                    break;
        }

    }
}







