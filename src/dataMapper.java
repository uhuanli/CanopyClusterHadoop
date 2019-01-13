import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class dataMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, LongWritable, Text>
{
	private LongWritable emKey = new LongWritable();
	private Text emVal = new Text();
	public void map(LongWritable key, Text values,
			OutputCollector<LongWritable, Text> output, Reporter reporter) 
		throws IOException 
	{
		String[] _line = (values.toString()).split(",");
		String _movie = _line[0];
		String _usr = _line[1];
		String _rating = _line[2];
		emKey.set(Integer.parseInt(_movie));
		emVal.set(_usr + " " + _rating);
		output.collect(emKey, emVal);
	}

}
