package org.equilibriums.hadoop.sample;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is a reducer that will get (in our case) combined word counts and 
 * and writes out total as text.
 * 
 * If we are not using job chaining (which is in our case) the output will be 
 * written into a job output file in with 'part-r-00000' naming.
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
