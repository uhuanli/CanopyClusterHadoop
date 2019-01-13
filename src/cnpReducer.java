import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class cnpReducer extends MapReduceBase 
implements Reducer<LongWritable, Text, LongWritable, Text>  
{
	private LongWritable emKey = new LongWritable();
	private Text emVal = new Text();
	private static List<mLink> cnpList = new ArrayList<mLink>();
	public void reduce(LongWritable _key, Iterator values,
			OutputCollector output, Reporter reporter) 
		throws IOException 
	{
		while(values.hasNext())
		{
			mLink _m = new mLink();
			String _val = values.next().toString();
			_m.setLink((int)_key.get(), _val);
			
			boolean _exist = false;;
			for(mLink im : cnpList)
			{
				if(im.calDist(_m) > cnpMapper.closeDist)
				{
					_exist = true;
					break;
				}
			}
			if(_exist)
			{
				reporter.incrCounter(Report.CANOPY_EXIST, 1);
				continue;
			}
			cnpList.add(_m);
			emKey.set(_key.get());
			_m.addCNP(_m.Movie);
			emVal.set(_m.formVal());
			output.collect(emKey, emVal);
			reporter.incrCounter(Report.CANOPY_NUMBER, 1);
		}
	}

}
