import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class HadoopPageRankResultReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		sort by hash map
//		double temp = 0.0;
//		temp =  key.get();
//	    Map<Double, Text> sortPR = new HashMap<>();
//		for (Text value : values) {
//			sortPR.put(temp, value);
//		 }
//		ArrayList<Double> list= new ArrayList<>(sortPR.keySet());
//		Collections.sort(list, new Comparator<Double>() {
//            @Override
//            public int compare(Double o1, Double o2) {
//                return o1>o2?-1:1;
//            }
//        });
//		
//		Iterator<Double> iterator = list.iterator();
//        Double rank = 0.0;
//        Text value = null;
//		while ((iterator.hasNext())){
//				rank = iterator.next();
//				value= sortPR.get(rank);
//	       }
//        context.write(new DoubleWritable(rank), new Text(value));
		double temp = 0.0;
		temp = key.get() * -1;
		for (Text value : values) {
			 context.write(value, new DoubleWritable(temp));
		 }
	}
}
