import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class cnpMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, LongWritable, Text> 
{
	public static int closeDist = 10;
	private LongWritable emKey = new LongWritable();
	private Text emVal = new Text();
	private static List<mLink> cnpList = new ArrayList<mLink>();
	public void map(LongWritable key, Text values,
			OutputCollector<LongWritable, Text> output, Reporter reporter) 
		throws IOException 
	{
		mLink _m = new mLink();
		_m.setLink((int)key.get(), values.toString());
		
		for(mLink im : cnpList)
		{
			if(im.calDist(_m) > closeDist)	return;
		}
		cnpList.add(_m);
		emKey.set(key.get());
		emVal.set(values.toString());
		output.collect(emKey, emVal);
	}

}
