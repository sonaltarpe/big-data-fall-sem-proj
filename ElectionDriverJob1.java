import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ElectionDriverJob1  {
	public static void main(String[] args) throws Exception {
		Job job = new Job();
	    job.setJarByClass(ElectionDriverJob1.class);
	    job.setJobName("Election Driver Job1");
 	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(ElectionMapperJob1.class);
	    job.setReducerClass(ElectionReducerJob1.class);
	    
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ElectionCompositeKey.class);
	    
	 /*   job.setNumReduceTasks(6);
		job.setPartitionerClass(ElectionDataPartition.class);
		//job.setCombinerClass(ElectionReducerJob1.class);
*/

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//job.getConfiguration().set("mapreduce.output.basename", "YearDistrict");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
