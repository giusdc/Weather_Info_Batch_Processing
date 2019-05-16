package utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;

public class HDFSUtils {
    /*public static FileSystem init(String[] pathlist) throws IOException {
        String hdfsuri = "hdfs://52.29.87.60:8020";



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

        String fileName=topic+".csv";
        //==== Write file
        String path="/user/hdfs/";
        Path newFolderPath= new Path(path);
        if(!fs.exists(newFolderPath)) {
            // Create new Directory
            fs.mkdirs(newFolderPath);
            System.out.println("Path "+path+" created.");
        }
        //Create a path
        Path hdfsreadpath = new Path(newFolderPath + "/" + fileName);

        //Init input stream
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        BufferedReader br=new BufferedReader(new InputStreamReader(inputStream));
        String line;
        File f = new File(System.getProperty("user.dir") +"/data/"+ topic + ".csv");
        BufferedWriter writer = null;
        try {
            f.createNewFile();
            writer = new BufferedWriter(new FileWriter(f));

        } catch (IOException e) {
            e.printStackTrace();

        }
        while ((line=br.readLine())!=null){
            writer.write(line);
        }
        //Classical input stream usage

        inputStream.close();
        fs.close();

    }*/
}
