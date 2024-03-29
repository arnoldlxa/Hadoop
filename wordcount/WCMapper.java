package hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
		    String[] words = StringUtils.split(line," ");
			
		    for (String word : words) {
				context.write(new Text(word), new LongWritable(1));
			}
		    
		    
			
		}
}
