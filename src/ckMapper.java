import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class ckMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, LongWritable, Text> 
{
	private LongWritable emKey = new LongWritable();
	private Text emVal = new Text();
	int confnumber = 0;
	private List<mLink> kcList = new ArrayList<mLink>();
	
	public void map(LongWritable key, Text values,
			OutputCollector<LongWritable, Text> output, Reporter reporter) 
		throws IOException 
	{
		mLink _im = new mLink();
		_im.setLink((int)key.get(), values.toString());		
		
		int maxCommon = -1;
		int _dist;
		int choseKC = -1;
//		找出距离最近的聚类并归属该聚类之下
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
		if(choseKC == -1)
		{
			reporter.incrCounter(Report.MAP_MISS, 1);
			reporter.incrCounter(Report.CANOPY_LOAD, kcList.size());
			return;
		}
		emKey.set(choseKC);
		emVal.set((int)key.get() + "\t" + values.toString());
		output.collect(emKey, emVal);
		reporter.incrCounter(Report.MAP_CENTER, 1);
	}
	
	public void configure(JobConf job)
	{
		if(!kcList.isEmpty())	return;
		SequenceFile.Reader reader = null;
		FileSystem fs;
		try 
		{
			Configuration _jcf = job;
			fs = FileSystem.get(_jcf);
//			通过标志值判断是否有第一次迭代，并通过相应的载入路径读取数据
			if(job.getLong("iterator", -1) == 1)
			{
				for(int i = 0; i < 8; i ++)
				{
					confnumber ++;
					Path path = new Path("kCenter/part-0000" + i);
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
			else
			{
				Path path = new Path("canopy/part-00000");
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
