import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ckReducer extends MapReduceBase 
implements Reducer<LongWritable, Text, LongWritable, Text>  
{

	int _ii = 0;
	
	public void reduce(LongWritable _key, Iterator values,
			OutputCollector output, Reporter reporter) 
	throws IOException 
	{
		LongWritable emKey = new LongWritable();
		Text emVal = new Text();
		List<mLink> movList = new ArrayList<mLink>();
		mLink _m;
		
		while (values.hasNext()) 
		{
			_m = new mLink();
			String[] _sp_val = (values.next().toString().split("\t"));
			_m.setLink(Integer.parseInt(_sp_val[0]), _sp_val[1]);
			movList.add(_m);
		}
		{
			if(movList.size() == 1)
			{
				reporter.incrCounter(Report.SINGLE_NUMBER, 1);
				reporter.incrCounter(Report.CLUSTER_NUMBER, 1);
				return;
			}
		}
		System.gc();//加紧回收内存
		TreeMap<Integer, Float> _ur = new TreeMap<Integer, Float>();
		_ii = 0;
		for(mLink _im : movList)
		{
			for(int _uid : _im.UR.keySet())
			{
				float _r = _im.UR.get(_uid);
				Float _d = _ur.get(_uid);
				if(_d != null)
				{
					_ur.put(_uid, _d.floatValue() + _r);
				}
				else
				{
					_ur.put(_uid, _r);
				}
			}
		}
		
		for(int _u : _ur.keySet())
		{
			_ur.put(_u, _ur.get(_u) / movList.size());
		}
		
		double sum_ur = 0.0;
		//一次性计算平均向量的模
		for(int _u : _ur.keySet())
		{
			float _r = _ur.get(_u);
			sum_ur += _r * _r;
		}
		double sqrt_sum_ur = Math.sqrt(sum_ur);
		double _max_sum = -1.0;
		double _similarity = 0.0;
		int _ikc = -1;
		//取离平均向量最近的点作为新的中心
		for(int i = 0; i < movList.size(); i ++)
		{
			_similarity = calSimilarity(_ur, movList.get(i), sqrt_sum_ur);
			if(_max_sum < _similarity)
			{
				_max_sum = _similarity;
				_ikc = i;
			}
		}
		{
			reporter.incrCounter(Report.CLUSTER_NUMBER, 1);
			reporter.incrCounter(Report.NUMBER_SIZE, 1);
			reporter.incrCounter(Report.TOTAL_SIZE, Runtime.getRuntime().totalMemory());
			reporter.incrCounter(Report.MAX_SIZE, Runtime.getRuntime().maxMemory());
			reporter.incrCounter(Report.FREESIZE, Runtime.getRuntime().freeMemory());
			reporter.incrCounter(Report.SIZE_MAP, _ur.size());
		}
		_ur = null;
		mLink _kc = movList.get(_ikc);
		movList = null;
		emKey.set(_kc.Movie);
		emVal.set(_kc.formVal());
		output.collect(emKey, emVal);	
		System.gc();
	}
	
	public double calSimilarity(TreeMap<Integer, Float> _sd, mLink _m, double sqrt_sum_sd)
	{
		double sumAB = 0.0;
		double sumA = 0.0;
		double sumB = sqrt_sum_sd;
		double dtmp = 0.0;
		for(int _uid : _m.UR.keySet())
		{
			dtmp = (float)_m.UR.get(_uid);
			sumAB += dtmp * _sd.get(_uid);
			sumA += dtmp * dtmp;
		}
		if(sumA == 0.0) return 0.0;
		return sumAB / (Math.sqrt(sumA) * sqrt_sum_sd);
	}

}
