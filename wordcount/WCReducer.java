package hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer  extends Reducer<Text , LongWritable, Text, LongWritable>{

		// <hello, {1,1,1}>
		@Override
		protected void reduce(Text key
				, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			
			long count =0;
			for( LongWritable value: values) {
				count +=value.get();
			}
			
			context.write(key, new LongWritable(count));
			
			
		}
}
