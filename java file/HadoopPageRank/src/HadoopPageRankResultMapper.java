import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HadoopPageRankResultMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		if ( value == null || value.getLength() == 0 ) {
    		return;
    	}
    	
        int tabIdx1 = value.find("\t");
        int tabIdx2 = value.find("\t", tabIdx1 + 1);
        String link = Text.decode(value.getBytes(), 0, tabIdx1);
        String pageRank = Text.decode(value.getBytes(), tabIdx1 + 1, tabIdx2 - (tabIdx1 + 1));
        double PR = -1*Double.parseDouble(pageRank.toString());
        context.write(new DoubleWritable(PR), new Text(link));
     }
}
