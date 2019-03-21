import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ElectionDriverJob2 {
	public static void main(String[] args) throws Exception {
		Job job = new Job();
	    job.setJarByClass(ElectionDriverJob2.class);
	    job.setJobName("Election Data Job2");
 	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(ElectionMapperJob2.class);
	    job.setReducerClass(ElectionReducerJob2.class);
	    
	    /*job.setNumReduceTasks(6);
		job.setPartitionerClass(electionPartition.class);*/

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//job.getConfiguration().set("mapreduce.output.basename", "YearDistrict");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}