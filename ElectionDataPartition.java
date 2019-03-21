import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class ElectionDataPartition extends Partitioner<Text, ElectionCompositeKey> {

	public int getPartition(Text key, ElectionCompositeKey value, int numReduceTasks) {
		String[] keyValues = key.toString().split(":");
		String year = keyValues[0];
		if (numReduceTasks == 0) {
			return 0;
		}
		if (year.equals("2016")) {
			return 0;
		} else if (year.equals("2014")) {
			return 1;
		} else if (year.equals("2012")) {
			return 2;
		} else if (year.equals("2010")) {
			return 3;
		} else if (year.equals("2008")) {
			return 4;
		} else {
			return 5;
		}
	}


}
