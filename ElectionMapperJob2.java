import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ElectionMapperJob2 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException 
	{
		{
			String[] rawValues = value.toString().split("\\t");
			if(rawValues.length==2){
				String KeyPair = rawValues[0];
				String ValuesPair = rawValues[1];
				context.write(new Text(KeyPair), new Text(ValuesPair));
			}
			
		}

	}
}