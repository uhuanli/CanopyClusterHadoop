import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class amReducer extends MapReduceBase 
implements Reducer<LongWritable, Text, LongWritable, Text>
{

	public void reduce(LongWritable _key, Iterator values,
			OutputCollector output, Reporter reporter)
	throws IOException 
	{
		while (values.hasNext()) 
		{
//			这步直接输出数据
			output.collect(_key, (Text)values.next());
		}
	}

}
