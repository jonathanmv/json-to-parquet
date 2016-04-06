package jonathanmv.json_to_parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.thrift.ParquetThriftOutputFormat;

import jonathanmv.json_to_parquet.model.FriendsEdge;

// Based on http://blog.cloudera.com/blog/2014/05/how-to-convert-existing-data-into-parquet/
public class Runner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Runner(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        final String output = "output";
        final String input = "input";
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);
        
        Job job = new Job(getConf());
        job.setJarByClass(getClass());
        Configuration conf = job.getConfiguration();
        
        final FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) fs.delete(outputPath, true);
                
        FileInputFormat.addInputPath(job, inputPath);

        job.setOutputFormatClass(ParquetThriftOutputFormat.class);
        ParquetThriftOutputFormat.setOutputPath(job, outputPath);
        ParquetThriftOutputFormat.setThriftClass(job, FriendsEdge.class);
        ParquetThriftOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        ParquetThriftOutputFormat.setCompressOutput(job, true);
        
        job.setMapperClass(JsonToThriftMapper.class);
        job.setNumReduceTasks(0);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

}