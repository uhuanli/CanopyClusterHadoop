import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class amMapper extends MapReduceBase 
implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, LongWritable, Text>  
{
	private List<mLink> cnpList = new ArrayList<mLink>();
	private LongWritable emKey = new LongWritable();
	private Text emVal = new Text();
	
	public void map(LongWritable key, Text values,
			OutputCollector<LongWritable, Text> output, Reporter reporter)
		throws IOException 
	{
		mLink _m = new mLink();
		_m.setLink((int)key.get(), values.toString());
		for(mLink _im : cnpList)
		{
			if(_m.calDist(_im) > cnpMapper.closeDist)
			{
				_m.addCNP(_im.Movie);
				reporter.incrCounter(Report.DUPLICATE_CANOPY, 1);
			}
		}
		emKey.set(_m.Movie);
		emVal.set(_m.formVal());
		output.collect(emKey, emVal);
	}

	
	public void configure(JobConf job)
	{
		if(!cnpList.isEmpty())	return;
		SequenceFile.Reader reader = null;
		FileSystem fs;
//		/*‘ÿ»ÎCanopy
		try 
		{
			Configuration _jcf = job;
			fs = FileSystem.get(_jcf);
			Path path = new Path("canopy/part-00000");
			SequenceFile.Reader sr = new SequenceFile.Reader(fs, path, _jcf);
			LongWritable _key = new LongWritable();Text _val = new Text();
			mLink _m;
			while(sr.next(_key, _val))
			{	
				_m = new mLink();
				_m.setLink((int)_key.get(), _val.toString());
				cnpList.add(_m);
			}	
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
