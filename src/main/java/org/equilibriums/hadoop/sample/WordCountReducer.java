package org.equilibriums.hadoop.sample;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer gets (in our case) combined word counts and writes out total as text.
 * 
 * The reducer will get at once all values that are collected from all the nodes for given key.
 * 
 * The output will be written into a job output file with 'part-r-00000' name format.
 *
 * @param <K>
 */
public class WordCountReducer<K> extends Reducer<K, IntWritable, K, Text> {

  public void reduce(K key, Iterable<IntWritable> values, Context context) 
  throws IOException, InterruptedException {
	  
	  Iterator<IntWritable> i = values.iterator();
      int count = 0;
      while ( i.hasNext() ) count += i.next().get();
            
      context.write( key, new Text("" + count) );
  }
}
