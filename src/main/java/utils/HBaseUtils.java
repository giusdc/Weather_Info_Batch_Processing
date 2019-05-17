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


    public static void simpleDataManipulationOperations(HBaseClient hbc) throws IOException, ServiceException, org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException {


        /* **********************************************************
         *  Data Management: Put, Get, Delete, Scan, Truncate
         * ********************************************************** */

        /* Create */
        System.out.println("\n******************************************************** \n");
        if (!hbc.exists("products")){
            System.out.println("Creating table...");
            hbc.createTable("products", "fam1", "fam2", "fam3");
        }
        System.out.println("\n******************************************************** \n");


        /* Put */
        System.out.println("\n ------------------\n");
        System.out.println("Adding value: row1:fam1:col1=val1");
        hbc.put("products", "row1", "fam1", "col1", "val1");
        System.out.println("Adding value: row1:fam1:col2=val2");
        hbc.put("products", "row1", "fam1", "col2", "val2");


        /* Get */
        String v1 = hbc.get("products",
                "row1", "fam1", "col1");
        String v2 = hbc.get("products",
                "row1", "fam1", "col2");
        System.out.println("Retrieving values : " + v1 + "; " + v2);
        System.out.println("\n ------------------\n");


        /* Scan table */
        System.out.println("Scanning table...");
        hbc.scanTable("products", null, null);
        System.out.println("\n ------------------\n");


        /* Update a row (the row key is unique) */
        System.out.println("Updating value: row1:fam1:col1=val1 to row1:fam1:col1=val3");
        hbc.put("products", "row1", "fam1", "col1", "val3");
        v1 = hbc.get("products", "row1", "fam1", "col1");
        v2 = hbc.get("products", "row1", "fam1", "col2");
        System.out.println("Retrieving values : " + v1 + "; " + v2);
        System.out.println("\n ------------------\n");


        /* Delete a column (with a previous value stored within) */
        System.out.println("Deleting value: row1:fam1:col1");
        v1 = hbc.get("products", "row1", "fam1", "col1");
        System.out.println("Retrieving value row1:fam1:col1 (pre-delete): " + v1);

        hbc.delete("products", "row1", "fam1", "col1");
        v1 = hbc.get("products", "row1", "fam1", "col1");
        System.out.println("Retrieving value row1:fam1:col1 (post-1st-delete): " + v1);

        hbc.delete("products", "row1", "fam1", "col1");
        v1 = hbc.get("products", "row1", "fam1", "col1");
        System.out.println("Retrieving value row1:fam1:col1 (post-2nd-delete): " + v1);
        System.out.println("\n ------------------\n");


        /* Scanning table */
        System.out.println("Scanning table...");
        hbc.scanTable("products", null, null);


        /* Truncate */
        System.out.println("Truncating data... ");
        hbc.truncateTable("products", true);
        System.out.println("\n ------------------\n");


        /* Scanning table */
        System.out.println("Scanning table...");
        hbc.scanTable("products", null, null);

    }

    public static void otherDataManipulationOperations(HBaseClient hbc) throws IOException, ServiceException {

        /* Create */
        System.out.println("\n******************************************************** \n");
        if (!hbc.exists("products")){
            System.out.println("Creating table...");
            hbc.createTable("products", "fam1", "fam2", "fam3");
        }
        System.out.println("\n******************************************************** \n");

        /* **********************************************************
         *  Data Management: Special Cases of Put, Delete
         * ********************************************************** */

        /* Try to insert a row for a not existing column family */
        System.out.println("Insert a key with a not existing column family");
        boolean res = hbc.put("products",
                "row2", "fam100", "col1", "val1");
        System.out.println(" result: " + res);
        System.out.println("\n ------------------\n");


        /* Delete: different columns, same column family */
        System.out.println(" # Inserting row2:fam1:col1");
        hbc.put("products", "row2", "fam1", "col1", "val1");
        System.out.println(" # Inserting row2:fam1:col2 ");
        hbc.put("products", "row2", "fam1", "col2", "val2");

        String v1 = hbc.get("products", "row2", "fam1", "col1");
        String v2 = hbc.get("products", "row2", "fam1", "col2");
        System.out.println("Retrieving values (pre-delete of col1): " + v1 + "; " + v2);

        System.out.println("Deleting data of different columns, but same column family... ");
        hbc.delete("products", "row2", "fam1", "col1");
        v1 = hbc.get("products", "row2", "fam1", "col1");
        v2 = hbc.get("products", "row2", "fam1", "col2");
        System.out.println("Retrieving values (post-delete of col1): " + v1 + "; " + v2);

        System.out.println("\n ------------------\n");


        /* Cleaning up all the data, before showing the next example */
        hbc.truncateTable("products", false);

        /* Delete: column family */
        System.out.println(" # Inserting row2:fam1:col2 = val2 (same family of existing row)");
        hbc.put("products", "row2", "fam1", "col2", "val2");
        System.out.println(" # Inserting row2:fam2:col3 = val3 (same family of existing row)");
        hbc.put("products", "row2", "fam2", "col3", "val3");
        System.out.println();

        System.out.println("Deleting a column family for a data... ");
        hbc.delete("products", "row2", "fam1", null);
        v1 = hbc.get("products", "row2", "fam1", "col1");
        v2 = hbc.get("products", "row2", "fam1", "col2");
        String v3 = hbc.get("products", "row2", "fam2", "col3");
        // note that we did not insert: v1 = row2:fam1:col1
        System.out.println("Retrieving values : " + v1 + "; " + v2 + "; " + v3);
        System.out.println("\n ------------------\n");


        /* Delete: entire row key */
        System.out.println("Deleting the whole row (row2)... ");
        hbc.delete("products",
                "row2", null, null);
        v1 = hbc.get("products", "row2", "fam1", "col1");
        v2 = hbc.get("products", "row2", "fam1", "col2");
        v3 = hbc.get("products", "row2", "fam2", "col3");
        System.out.println("Retrieving values : " + v1 + "; " + v2 + "; " + v3);

    }



    public static void getAllVersions(HBaseClient hbc) throws IOException, ServiceException {
        /* Create */
        System.out.println("\n******************************************************** \n");
        if (!hbc.exists("products")){
            System.out.println("Creating table...");
            hbc.createTable("products", "fam1", "fam2", "fam3");
        }
        System.out.println("\n******************************************************** \n");


        /* Put */
        System.out.println("\n ------------------\n");
        System.out.println("Adding value: row1:fam1:col1=val1");
        hbc.put("products", "row1", "fam1", "col1", "val1");
        System.out.println("Adding value: row1:fam1:col1=val2");
        hbc.put("products", "row1", "fam1", "col1", "val2");

        /* Get All Versions */
        System.out.println("Retrieving all versions of row1:fam1:col1");
        Map<Long, String> values = hbc.getAllVersions("products", "row1", "fam1", "col1");

        for (long ts : values.keySet()){
            System.out.println(" - " + ts + ": " + values.get(ts));
        }


        // Deleting a value an trying again:
        System.out.println("Deleting value of row1:fam1:col1");
        hbc.delete("products", "row1", "fam1", "col1");
        values = hbc.getAllVersions("products", "row1", "fam1", "col1");
        System.out.println("Retrieving all versions of row1:fam1:col1");
        for (long ts : values.keySet()){
            System.out.println(" - " + ts + ": " + values.get(ts));
        }

        //  Drop table
        System.out.println("Deleting table...");
        hbc.dropTable("products");

    }


    public static void filter(HBaseClient hbc) throws IOException, ServiceException, org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException {


        /* **********************************************************
         *  Data Management: Filter
         * ********************************************************** */


        /* Create */
        System.out.println("\n******************************************************** \n");
        if (!hbc.exists("products")){
            System.out.println("Creating table...");
            hbc.createTable("products", "fam1", "fam2", "fam3");
        }
        System.out.println("\n******************************************************** \n");


        /* Put: different rows, same column family */
        System.out.println(" # Inserting row1:fam1:col1");
        hbc.put("products", "row1", "fam1", "col1", "test");
        System.out.println(" # Inserting row2:fam1:col1 ");
        hbc.put("products", "row2", "fam1", "col1", "val");

        String v1 = hbc.get("products", "row1", "fam1", "col1");
        String v2 = hbc.get("products", "row2", "fam1", "col1");
        System.out.println("Retrieving values: " + v1 + "; " + v2);



        Table table = hbc.getConnection().getTable(TableName.valueOf("products"));
        Scan scan = new Scan();

        // We add to the scan the column we are interested in and the column we want to filter on
        scan.addColumn(b("fam1"), b("col1"));


        // We specify the row prefix for starting the scan operation
        scan.withStartRow(b("row"))
                // we set the stop condition as follows: proceed until scanning all rows with specified prefix
                .setRowPrefixFilter(b("row"));


        SingleColumnValueFilter valueFilter =
                new SingleColumnValueFilter(
                        b("fam1"),
                        b("col1"),
                        CompareFilter.CompareOp.NOT_EQUAL,
                        b("val"));


        scan.setFilter(valueFilter);


        ResultScanner scanner = table.getScanner(scan);


        // Emitting results
        int count = 0;
        for (Result r = scanner.next(); r != null; r = scanner.next()){

            byte[] test = r.getValue(b("fam1"), Bytes.toBytes("col1"));
            System.out.println(" - " + new String(r.getRow()) + ", Test result = " + new String(test));
            count++;
        }
        System.out.println(" Found: " + count + " entries");

        scanner.close();


        //  Drop table
        System.out.println("Deleting table...");
        hbc.dropTable("products");


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

            case 2:
                /* **********************************************************
                 *  Data Management: Put, Get, Delete, Scan, Truncate
                 * ********************************************************** */
                simpleDataManipulationOperations(hbc);
                break;

            case 3:

                /* Data manipulation, special cases */
                otherDataManipulationOperations(hbc);
                break;


            case 4:
                /* Get All Versions */
                getAllVersions(hbc);
                break;


            default:
                /* **********************************************************
                 *  Data Management: Filter
                 * ********************************************************** */
                filter(hbc);
                break;


        }

    }

    public static void execute(String pathHDFS,int query, int idFile) throws IOException {

        String hdfsuri="hdfs://3.122.52.163:8020";
        HBaseClient hbc = new HBaseClient();
        delete(hbc,query);
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
        switch (query){
            case 1:
                result=line.split(",",2);
                hbc.put("query1", result[0], "fam1", "Lista città", result[1].substring(0,result[1].length()-1));
                 v1= hbc.get("query1", result[0], "fam1", "Lista città");
                System.out.println(v1);
            case 2:
                String familyName=getFamilyById(idFile);
                result=line.split(",",2);
                String[]value=result[1].split(",");

                hbc.put("query2", result[0], familyName, "mean",value[0].substring(1));
                hbc.put("query2", result[0], familyName, "std dev",value[1]);
                hbc.put("query2", result[0], familyName, "min",value[2]);
                hbc.put("query2", result[0], familyName, "max",value[3].substring(0,value[3].length()-1));

                v1 = hbc.get("query2", result[0], familyName, "min");
                System.out.println("Sto"+v1);
            case 3:
                result=line.split(",",2);
                hbc.put("query1", result[0], "fam1", "Lista città", result[1].substring(0,result[1].length()-1));
                v1 = hbc.get("query1", result[0], "fam1", "Lista città");
                System.out.println(v1);

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
                    hbc.createTable("query3","fam1");
                }
                break;
                default:
                    break;
        }

    }
}







