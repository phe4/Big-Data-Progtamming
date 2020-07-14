
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopAvgTemp {
	public static class AvgTempMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static final int MISSING = 9999;
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
	
			String line = value.toString();
			String year = line.substring(15, 19);
			int airTemperature;
			if (line.charAt(87) == '+') {
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}
		
			String quality = line.substring(92, 93);
			if (airTemperature != MISSING && quality.matches("[01459]")) {
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}
	}
	public static class AvgTempReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for(IntWritable value : values) {
				sum = sum+value.get();
				count++;
			}
			if(count != 0) {
				context.write(key, new IntWritable(sum/count));
			}
		}
	}
	
	public static Logger log = Logger.getLogger(HadoopAvgTemp.class);
	
	public static void main(String[] args) throws Exception {
		int N = 1;
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Average Temprature");
		job.setJarByClass(HadoopAvgTemp.class);
		job.setMapperClass(AvgTempMapper.class);
		job.setNumReduceTasks(N);
		job.setReducerClass(AvgTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		log.info(job.getJobName());
		log.info("The number of reducer tasks:" + N);
		log.info("The partitioner class that is being used: "+job.getPartitionerClass().getName());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}