
import java.io.IOException;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopMatrix {
	public static class MatrixMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString(); 
			String[] values = line.toString().split(",");
			String rindex = values[0];
			String cindex = values[1];
			String evalue = values[2];
			context.write(new Text(cindex), new Text(rindex+","+cindex+","+evalue));
		}
	}
	public static class MatrixReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			for(Text val : values) {
				String[] str = val.toString().split(",");
				int cind = Integer.parseInt(str[1]);
				String cindex = cind/3+"";
				Text t = new Text(cindex);
				context.write(t, val);
			}
		}
	}
	
	public static class MatrixPartitioner extends
			Partitioner<Text, Text>{

		@Override
		public int getPartition(Text key, Text value, int number) {
			
			String[] str = value.toString().split(",");
			int cind = Integer.parseInt(str[1]);
			if(cind<=2) {
				return 0;
			}
			else if(cind>2&&cind<=5) {
				return 1%number;
			}
			else {
				return 2%number;
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Split Matrix");
		job.setJarByClass(HadoopMatrix.class);
		job.setMapperClass(MatrixMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(MatrixPartitioner.class);
		job.setNumReduceTasks(3);
		job.setReducerClass(MatrixReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}