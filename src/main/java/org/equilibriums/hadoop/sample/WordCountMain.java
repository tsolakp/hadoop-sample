package org.equilibriums.hadoop.sample;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * The famous MapReduce word count example for Hadoop.
 * 
 * The application expects 2 arguments. First is the input path and second the output path.
 * 
 * If this is run as standalone application (in Eclipse for example) you can pass
 * any full Windows or Unix paths for each argument. 
 * For example: java WordCountMain C:\hadoop\data\input C:\hadoop\data\output
 *  
 * If this is run within Hadoop (like in pseudo mode with hadoop jar command) than the paths 
 * should be in HDFS system.
 * For example if you created two HDFS directories called 'input' and 'ouput' with command
 * like 'hadoop dfs -mkdir /data/input' than the paths will be '/data/input' and '/data/output'.
 * 
 * The 'output' directory should not exist when we run Hadoop otherwise it will give an error.
 */
public class WordCountMain extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountMain(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        
        if (remainingArgs.length < 2) {
            System.err.println("Usage: WordCountMain <in> <out>");
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }
        
        Job job = new Job(getConf(), "WordCountMain");
        job.setJarByClass( getClass() );
             
        //configure Hadoop to use user passed 2 arguments as input and output
        //directories.
        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        
        //here we define what mapper is going to output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        //here we define what reducer is going to output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        //set Mapper, Combiner and Reducer classes so that Hadoop
        //will instantiate and run them across nodes.
        //When run standalone they all will run in one VM.
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(WordCountReducer.class);
           
        boolean success = job.waitForCompletion(true);
        
        return success ? 0 : 1;
    }
}
