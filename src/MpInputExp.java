import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MpIntputExp {
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		String[] typeInArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (typeInArgs.length != 4) {
			System.err.println("Usage:    ");
			System.exit(2);
		}

		Path outputPath = new Path(typeInArgs[3]);
		FileSystem hdfs = outputPath.getFileSystem(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		Job exampleJob = new Job(conf, "example");
		exampleJob.setJarByClass(MpIntputExp.class);

		MultipleInputs.addInputPath(exampleJob, new Path(typeInArgs[0]), TextInputFormat.class, firstMapper.class);
		MultipleInputs.addInputPath(exampleJob, new Path(typeInArgs[1]), TextInputFormat.class, secondMapper.class);
		MultipleInputs.addInputPath(exampleJob, new Path(typeInArgs[2]), TextInputFormat.class, thirdMapper.class);

		exampleJob.setReducerClass(Reduce.class);
		exampleJob.setMapOutputKeyClass(Text.class);
		exampleJob.setMapOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(exampleJob, outputPath);

		System.exit(exampleJob.waitForCompletion(true) ? 0 : 1);
	}

	public static class firstMapper extends Mapper {
		public void map(LongWritable key, Text value, Context context) throws Exception {
			String Key = value.toString().split(";")[1];
			int Value = Integer.parseInt(value.toString().split(";")[2]);
			context.write(new Text(Key), new IntWritable(Value));
		}
	}

	public static class secondMapper extends Mapper {
		String line;

		public void map(LongWritable key, Text value, Context context) throws Exception {
			String line = value.toString().replaceAll("[{}]", "");
			String Key = line.split(",")[1].split(":")[1];
			int Value = Integer.parseInt(line.split(",")[2].split(":")[1]);
			context.write(new Text(Key), new IntWritable(Value));
		}
	}

	public static class thirdMapper extends Mapper {
		String line;

		public void map(LongWritable key, Text value, Context context) throws Exception {
			String line = value.toString().replaceAll("[]\\[]", "");
			String Key = line.split(";")[1].split("=>")[1];
			int Value = Integer.parseInt(line.split(";")[2].split("=>")[1]);
			context.write(new Text(Key), new IntWritable(Value));
		}
	}

	public static class Reduce extends Reducer {
		public void reduce(Text key, Iterable value, Context context) throws Exception {
			int numerator = 0;
			int denominator = 0;
			for (IntWritable v : value) {
				numerator += v.get();
				denominator++;
			}
			int avg = numerator / denominator;
			context.write(key, new IntWritable(avg));
		}
	}
}