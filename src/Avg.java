import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class Avg {

	// map步驟
	public static class Map extends Mapper {

		// 撰寫map方法
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();// 數據先轉成字串(String)
			StringTokenizer script = new StringTokenizer(line, "\n");// 分割數據
			while (script.hasMoreElements()) {
				StringTokenizer scriptLine = new StringTokenizer(script.nextToken());
				// 將Key和Value用逗號(",")分開
				Text Name = new Text(scriptLine.nextToken(","));
				int Score = Integer.parseInt(scriptLine.nextToken(","));
				context.write(Name, new IntWritable(Score));
			}
		}
	}

	// reduce 步驟
	public static class Reduce extends Reducer {
		// 撰寫reduce方法
		public void reduce(Text key, Iterable value, Context context) throws IOException, InterruptedException {
			int numerator = 0;// 初始化分子
			int denominator = 0;// 初始化分母
			for (IntWritable v : value) {
				numerator += v.get();// 分子累加
				denominator++;// 分母每次+1
			}
			int avg = numerator / denominator;// 相除
			context.write(key, new IntWritable(avg));
		}
	}

	// 主程式
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Path dst_path = new Path(otherArgs[1]);
		FileSystem hdfs = dst_path.getFileSystem(conf);

		// 檢查OUTPUT的路徑是否存在，有的話就宰了他!
		if (hdfs.exists(dst_path)) {
			hdfs.delete(dst_path, true);
		}
		;

		Job job = new Job(conf, "Avg");
		job.setJarByClass(Avg.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}