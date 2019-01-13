import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

enum Report
{
	MOVIE_NUMBER, 
	CLUSTER_NUMBER, 
	CANOPY_NUMBER, 
	SINGLE_NUMBER,
	DUPLICATE_CANOPY, 
	MAP_CENTER, 
	MAP_MISS, 
	IN_REDUCER, 
	CANOPY_EXIST,
	DIFF_MAP_CENTER, 
	CANOPY_LOAD, 
	MAX_SIZE, 
	FREESIZE, 
	TOTAL_SIZE, 
	NUMBER_SIZE, 
	SIZE_MAP
}

public class ckDriver {
	
	public static final  String _center_path = "kCenter";
	public static void main(String[] args) throws IOException 
	{
//		/*预处理数据
		{
			JobClient preData_client = new JobClient();
			JobConf preData_conf = new JobConf(ckDriver.class);
	
			// TODO: specify output types
			preData_conf.setOutputKeyClass(LongWritable.class);
			preData_conf.setOutputValueClass(Text.class);
//			preData_conf.setInputFormat(SequenceFileInputFormat.class);
			preData_conf.setOutputFormat(SequenceFileOutputFormat.class);
			// TODO: specify input and output DIRECTORIES (not files)
			SequenceFileInputFormat.addInputPath(preData_conf, new Path("/public/netflix/")); 
			SequenceFileOutputFormat.setOutputPath(preData_conf, new Path("ckData/"));
	
			preData_conf.setMapperClass(dataMapper.class);	
			preData_conf.setReducerClass(dataReducer.class);
			preData_conf.setNumMapTasks(16);
			preData_conf.setNumReduceTasks(6);
	
			preData_client.setConf(preData_conf);
			try {
				RunningJob _job = JobClient.runJob(preData_conf);
				Counters CT = _job.getCounters();
				long _number_movie = CT.getCounter(Report.MOVIE_NUMBER);
				System.out.print("Movie Number: " + _number_movie + "\n");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
//		*/
		
//		/*求出Canopy对应的中心点集
		{
			JobClient cnp_client = new JobClient();
			JobConf cnp_conf = new JobConf(ckDriver.class);
	
			// TODO: specify output types
			cnp_conf.setOutputKeyClass(LongWritable.class);
			cnp_conf.setOutputValueClass(Text.class);
			cnp_conf.setInputFormat(SequenceFileInputFormat.class);
			cnp_conf.setOutputFormat(SequenceFileOutputFormat.class);
			// TODO: specify input and output DIRECTORIES (not files)
			SequenceFileInputFormat.addInputPath(cnp_conf, new Path("ckData/")); 
			SequenceFileOutputFormat.setOutputPath(cnp_conf, new Path("canopy/"));
	
			cnp_conf.setMapperClass(cnpMapper.class);	
			cnp_conf.setReducerClass(cnpReducer.class);
			cnp_conf.setNumMapTasks(8);
			cnp_conf.setNumReduceTasks(1);
	
			cnp_client.setConf(cnp_conf);
			try {
				RunningJob _job = JobClient.runJob(cnp_conf);
				Counters CT = _job.getCounters();
				long _number_canopy = CT.getCounter(Report.CANOPY_NUMBER);
				long _canopy_exist = CT.getCounter(Report.CANOPY_EXIST);
				System.out.print("Canopy Number: " + _number_canopy + "\n" + "Canopy Exist: " + _canopy_exist);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
//		*/
		
//		/*分配Canopy到每个Movie
		{
			JobClient am_client = new JobClient();
			JobConf am_conf = new JobConf(ckDriver.class);
	
			// TODO: specify output types
			am_conf.setOutputKeyClass(LongWritable.class);
			am_conf.setOutputValueClass(Text.class);
			am_conf.setInputFormat(SequenceFileInputFormat.class);
			am_conf.setOutputFormat(SequenceFileOutputFormat.class);
			// TODO: specify input and output DIRECTORIES (not files)
			SequenceFileInputFormat.addInputPath(am_conf, new Path("ckData/")); 
			SequenceFileOutputFormat.setOutputPath(am_conf, new Path("assignMovie/"));
	
			am_conf.setMapperClass(amMapper.class);	
			am_conf.setReducerClass(amReducer.class);
			am_conf.setNumMapTasks(16);
			am_conf.setNumReduceTasks(5);
	
			am_client.setConf(am_conf);
			try {
				RunningJob _job = JobClient.runJob(am_conf);
				Counters CT = _job.getCounters();
				long _number_duplicate = CT.getCounter(Report.DUPLICATE_CANOPY);
				System.out.print("Duplicate Number: " + _number_duplicate + "\n");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
//		*/
		
//		/*进行10次Kmeans迭代
		{
			String _input;
			String _output;
			String _center;
			_input = "assignMovie/";
			_output = "outputCenter";
//			int i = 0;
			for(int i = 0; i < 10; i ++)
			{					
				{
					JobClient ckIterator = new JobClient();
					JobConf ckConf = new JobConf(ckDriver.class);
					if(i == 0)
					{
						ckConf.setLong("iterator", 0);						
					}
					else
					{
						ckConf.setLong("iterator", 1);
					}

					
					// TODO: specify output types
					ckConf.setOutputKeyClass(LongWritable.class);
					ckConf.setOutputValueClass(Text.class);
			
					
					ckConf.setInputFormat(SequenceFileInputFormat.class);
					ckConf.setOutputFormat(SequenceFileOutputFormat.class);
					// TODO: specify input and output DIRECTORIES (not files)
					SequenceFileInputFormat.addInputPath(ckConf, new Path(_input)); 
					SequenceFileOutputFormat.setOutputPath(ckConf, new Path(_output));
					FileSystem fs = FileSystem.get(ckConf);
					fs.delete(new Path(_center_path));
					fs.rename(new Path(_output), new Path(_center_path));
					
					ckConf.setMapperClass(ckMapper.class);			
					ckConf.setReducerClass(ckReducer.class);	
					ckIterator.setConf(ckConf);				
					ckConf.setNumMapTasks(16);
					ckConf.setNumReduceTasks(8);
					/*
					 * 设置内存限制
					 */
					ckConf.set("mapred.child.java.opts","-Xmx1536m");
					try {
						RunningJob _job = JobClient.runJob(ckConf);
						Counters CT = _job.getCounters();
						long _number_cluster = CT.getCounter(Report.CLUSTER_NUMBER);
						long _number_single = CT.getCounter(Report.SINGLE_NUMBER);
						long _map_center = CT.getCounter(Report.MAP_CENTER);
						long _map_miss = CT.getCounter(Report.MAP_MISS);
						long _canopy_load = CT.getCounter(Report.CANOPY_LOAD);
						long _max_size = CT.getCounter(Report.MAX_SIZE);
						long _size_map = CT.getCounter(Report.SIZE_MAP);
						System.out.print("cluster number : " + _number_cluster + "\n"
								+ "single number: " + _number_single + "\n" + "map c: " + _map_center
								+ "\nmap miss" + _map_miss + "\n" + "canopy load: " + _canopy_load + "\n"
								+ "max size: " + _max_size + "\n" + "size map: " + _size_map + "\n");
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
			}
		}
//		*/
		
//		/*清理数据得到清晰的聚类结果
		{
			JobClient clean_client = new JobClient();
			JobConf clean_conf = new JobConf(ckDriver.class);
	
			// TODO: specify output types
			clean_conf.setOutputKeyClass(LongWritable.class);
			clean_conf.setOutputValueClass(LongWritable.class);
			clean_conf.setInputFormat(SequenceFileInputFormat.class);
//			clean_conf.setOutputFormat(SequenceFileOutputFormat.class);
			// TODO: specify input and output DIRECTORIES (not files)
			SequenceFileInputFormat.addInputPath(clean_conf, new Path("assignMovie/")); 
			SequenceFileOutputFormat.setOutputPath(clean_conf, new Path("Cluster/"));
	
			clean_conf.setMapperClass(cleanMapper.class);	
			clean_conf.setReducerClass(cleanReducer.class);
			clean_conf.setNumMapTasks(16);
			clean_conf.setNumReduceTasks(6);
	
			clean_client.setConf(clean_conf);
			try {
				RunningJob _job = JobClient.runJob(clean_conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}

