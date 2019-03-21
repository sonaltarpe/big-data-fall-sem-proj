import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ElectionReducerJob1 extends
		Reducer<Text, ElectionCompositeKey, Text, Text> {

	public void reduce(Text key, Iterable<ElectionCompositeKey> values,
			Context context) throws IOException, InterruptedException {
		int totalVotingCount = 0, totalReg = 0, population = -1;
		ArrayList<Integer> totalVotingList = new ArrayList<Integer>();
		for (ElectionCompositeKey value : values) {
			String[] allValues = value.right.toString().split(":");
			if (value.left == 1) {
				
				int totalVoting = Integer.parseInt(allValues[0]);
				totalVotingList.add(totalVoting);
				
			}
			else{
				population = Integer.parseInt(allValues[1]);
			}
		}
		for(Integer value : totalVotingList) {
			totalVotingCount = totalVotingCount + value;
		}
		context.write(key, new Text(totalVotingCount + ":" + population));
	}
}