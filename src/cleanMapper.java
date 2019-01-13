import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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


public class cleanMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, LongWritable, LongWritable>  
{
	private LongWritable emKey = new LongWritable();
	private LongWritable emVal = new LongWritable();
	private List<mLink> kcList = new ArrayList<mLink>();
	
	public void map(LongWritable key, Text values,
			OutputCollector<LongWritable, LongWritable> output, Reporter reporter) 
		throws IOException 
	{
		mLink _im = new mLink();
		_im.setLink((int)key.get(), values.toString());		
		
		int maxCommon = -1;
		int _dist;
		int choseKC = -1;
		for(mLink _kc : kcList)
		{
			if(_kc.commonCanopy(_im))
			{
				_dist = _kc.calDist(_im);
				if(maxCommon < _dist)
				{
					maxCommon = _dist;
					choseKC = _kc.Movie;
				}
			}
		}
//		记录丢失情况
		if(choseKC == -1)
		{
			reporter.incrCounter(Report.MAP_MISS, 1);
			return;
		}
		emKey.set(choseKC);
		emVal.set((int)key.get());
		output.collect(emKey, emVal);
	}
	
	public void configure(JobConf job)
	{
//		载入kCenter数据
		if(!kcList.isEmpty())	return;
		SequenceFile.Reader reader = null;
		FileSystem fs;
		try 
		{
			Configuration _jcf = job;
			fs = FileSystem.get(_jcf);
			for(int i = 0; i < 8; i ++)
			{
				Path path = new Path("outputCenter/part-0000" + i);
				SequenceFile.Reader sr = new SequenceFile.Reader(fs, path, _jcf);
				LongWritable _key = new LongWritable(); Text _val = new Text();
				mLink _kc;
				while(sr.next(_key, _val))
				{	
					_kc = new mLink();
					_kc.setLink((int)_key.get(), _val.toString());
					kcList.add(_kc);
				}					
			}
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
