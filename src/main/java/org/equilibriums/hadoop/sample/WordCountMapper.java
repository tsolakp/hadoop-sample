package org.equilibriums.hadoop.sample;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * Turns lines from a text file into (word, 1) tuples.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
	private static final IntWritable ONE = new IntWritable(1);
	
    /**
     * Split line string separated by ',' and pass to combiner/reducer to do sum of their count.
     * 
     * @param value - the whole line String in the file
     * 
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    protected void map(LongWritable offset, Text value, Context context) 
    throws IOException, InterruptedException {
            	    	
        StringTokenizer tok = new StringTokenizer(value.toString(), ",");
                
        while ( tok.hasMoreTokens() ) {
            Text word = new Text( tok.nextToken() );
            //to avoid creating new IntWritable(1) we will use singleton here.
            //Mappers, Combiners and Reducers are thread safe 
            context.write( word, ONE );            
        }        
    }
}
