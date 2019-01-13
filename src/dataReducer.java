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


public class dataReducer extends MapReduceBase 
implements Reducer<LongWritable, Text, Text, IntWritable>  
{
	private Text tVal = new Text();
	public void reduce(LongWritable _key, Iterator values,
			OutputCollector output, Reporter reporter) 
	throws IOException 
	{
//		String _val = "";
		int _len = 0;
		StringBuffer _val = new StringBuffer();
		while(values.hasNext())
		{
			_len ++;
			_val.append(" ").append(((Text)values.next()).toString());
		}
		tVal.set(_len + " " + _val.toString() + " " + "0");
		output.collect(_key, tVal);
		reporter.incrCounter(Report.MOVIE_NUMBER, 1);
	}

}
