package de.uni_mannheim.desq.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;

import java.io.IOException;
import java.io.InputStream;

public class IoUtils {
    /** Returns an input stream to read a file from any Hadoop-supported file system (e.g., HDFS or the local file
     * system). */
    public static InputStream readHadoop(String fileName, SparkContext sc) throws IOException {
        // Get Hadoop configuration from Spark Context
        Configuration conf = sc.hadoopConfiguration();

        // Get information about the file system of the given path
        Path path = new Path(fileName);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataInputStream inputStream = fs.open(path);
        return inputStream;
    }
}
