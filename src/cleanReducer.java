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


public class cleanReducer extends MapReduceBase 
implements Reducer<LongWritable, LongWritable, LongWritable, Text>   
{

	public void reduce(LongWritable _key, Iterator values,
			OutputCollector output, Reporter reporter) 
	throws IOException 
	{
		StringBuffer _val = new StringBuffer(100000);
//		输出清晰的聚类结果
		while (values.hasNext()) 
		{
			_val.append("[" + ((LongWritable)values.next()).get() + "]\t");
		}
		output.collect(_key, new Text(_val.toString()));
	}

}
