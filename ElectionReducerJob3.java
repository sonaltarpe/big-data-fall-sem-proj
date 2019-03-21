import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ElectionReducerJob3 extends
Reducer<Text, IntWritable, Text, Text> {

public void reduce(Text key, Iterable<IntWritable> values,
	Context context) throws IOException, InterruptedException {
 int totalSum = 0,count=0, min=0, max=0;

  for (IntWritable value : values) {
	   int currentValue =value.get();
	   totalSum += currentValue;
	   if(count==0)
	   {
		   min=currentValue;
		   max=currentValue;
	   }
	   else
	   {
		   if(min>=currentValue)
			  min=currentValue;
		   if(max<=currentValue)
			  max=currentValue;
	   }
	   count++;
    } 
    if(count > 1)
    {   
    double averageTurnout = (double)totalSum /(double)count;
    context.write(key, new Text(min+"|"+Math.round(averageTurnout)+"|"+max));

}
}

}
