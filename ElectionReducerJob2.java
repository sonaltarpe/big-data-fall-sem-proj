import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ElectionReducerJob2  extends
Reducer<Text, Text, Text, Text> {

public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException {
float percentage = 0.0f;
int totalVoting=0;
String str = "";
for (Text value : values) {
	String[] allValues = value.toString().split(":");
	//str =str+value;
	if(allValues.length==2){
		 totalVoting = Integer.parseInt(allValues[0]);
		  int totalPopulation= Integer.parseInt(allValues[1]);
		percentage = (100*1.0f * totalVoting) / totalPopulation;
	}
}

context.write(key, new Text(percentage+"%   "+totalVoting));
}
}
