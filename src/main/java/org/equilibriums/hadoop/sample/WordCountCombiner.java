package org.equilibriums.hadoop.sample;

import java.util.Iterator;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner is like Reducer which aggregates the output from Mappers but does it
 * against local Mappers vs. Reducer which works across multiple nodes.
 * 
 * Combiner should have same output key/value types as Mapper output key/value types,
 * but can be different than the Reducer's output key/value types.
 * This is because Reducer expect output key/value type from Mapper and does not
 * know that there will be Combiner that will change the types.
 * 
 * This is generic 'sum' combiner that does not care what type input key and value
 * are. It just count number of values for given key passed from Mappers 
 * (because it assumes that Mappers will always emit (word, 1) tuples) and writes
 * to output which will be passed to actual reducer.
 *
 * @param <Key>
 */
public class WordCountCombiner<K, V> extends Reducer<K, V, K, IntWritable> {
	
  public void reduce(K key, Iterable<V> values, Context context) 
  throws IOException, InterruptedException {
	  
      Iterator<V> i = values.iterator();
      int count = 0;
      while ( i.hasNext() ){
    	  i.next();
    	  //We can safely assume that i.next will always give us '1'.
    	  //We could very well replace V with IntWritable and replace 
    	  //this code with count += i.next().get();
    	  count += 1;
      }            
      context.write( key, new IntWritable(count) );
  }
}
