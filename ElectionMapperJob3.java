import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ElectionMapperJob3 extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	int voting=0,percentage=0,totalVoting=0;
        String District="";
        String[] rawValues = value.toString().split("\\t|:|\\s+");
        
    	if (rawValues.length == 4) {
            District = rawValues[1];
            totalVoting =Integer.parseInt(rawValues[3]);
    		context.write(new Text(District), new IntWritable(totalVoting));
    	}
    }
}
