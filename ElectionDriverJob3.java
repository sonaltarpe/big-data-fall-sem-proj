import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ElectionDriverJob3 {
	public static void main(String[] args) throws Exception {
		Job job = new Job();
	    job.setJarByClass(ElectionDriverJob3.class);
	    job.setJobName("Election Data Job3");
 	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(ElectionMapperJob3.class);
	    job.setReducerClass(ElectionReducerJob3.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//job.getConfiguration().set("mapreduce.output.basename", "YearDistrict");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}

