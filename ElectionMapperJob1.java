import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ElectionMapperJob1 extends Mapper<LongWritable, Text, Text, ElectionCompositeKey> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] rawLine = value.toString().split(",");
		String regAtPolling="",District="",reg7AM="",totalVoting="",fileID="";
		String votingCount="",population="";
		String[] DistrictValues;
		int joinID=-1;
		if (rawLine.length == 52) {
			 District = rawLine[6];
			 votingCount = rawLine[24];
			 fileID="2016";
			 population=null;
			 joinID=1;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
		}
		else if (rawLine.length == 68) {
			 District = rawLine[6];
			 votingCount = rawLine[23];
			 fileID="2014";
			 population=null;
			 joinID=1;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
		}
		else if (rawLine.length == 67) {
			 District = rawLine[5];
			 votingCount = rawLine[23];
			 fileID="2012";
			 population=null;
			 joinID=1;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
		}
		else if (rawLine.length == 61) {
			 District = rawLine[2];
			 votingCount = rawLine[19];
			 fileID="2010";
			 population=null;
			 joinID=1;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
		}
		else if (rawLine.length == 55) {
			 District = rawLine[2];
			 votingCount = rawLine[20];
			 fileID="2008";
			 population=null;
			 joinID=1;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
		}
		else if (rawLine.length == 62) {
			 District = rawLine[2];
			 votingCount = rawLine[18];
			 fileID="2006";
			 population=null;
			 joinID=1;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
		}
		/*else if (rawLine.length == 7) {
		
			 DistrictValues = rawLine[3].split(" ");
			 District=DistrictValues[2];
			 votingCount = null;
			 fileID=rawLine[0];
			 population=rawLine[5];
			 joinID=2;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
	}*/
		else if (rawLine.length == 328 || rawLine.length == 329 || rawLine.length == 341) {
			
			 DistrictValues = rawLine[3].split(" ");
			 District=DistrictValues[2];
			 votingCount = null;
			 fileID=rawLine[0];
			 population=rawLine[73];
			 joinID=2;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
		}
		
		else if (rawLine.length > 400 && rawLine[0].equals("2008")) {
			
			 DistrictValues = rawLine[4].split(" ");
			 District=DistrictValues[2];
			 votingCount = null;
			 fileID=rawLine[0];
			 population=rawLine[111];
			 joinID=2;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
			 context.write(new Text(rawLine[1]+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+rawLine[114]));
			 
		}
		/*else if (rawLine.length > 400 && rawLine[1].equals("2006")) {
			
			 DistrictValues = rawLine[4].split(" ");
			 District=DistrictValues[2];
			 votingCount = null;
			 fileID=rawLine[1];
			 population=rawLine[114];
			 joinID=2;
			 context.write(new Text(fileID+":"+District), new ElectionCompositeKey(joinID,
					 votingCount +":"+population));
			 
		}*/
			 
		
	
		}
	}