package Hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmeans {
	public static class PartitionMapper extends Mapper<LongWritable, Text, Text, Text> {
		HashMap<String, double[]> mCenters = new HashMap<String, double[]>();

		public void setup(Context context) {
			try {
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if (null != cacheFiles && cacheFiles.length > 0) {
					String line;
					BufferedReader br;
					for (int i = 0; i < cacheFiles.length; i++) {
						br = new BufferedReader(new FileReader(cacheFiles[i].toString()));
						try {
							while ((line = br.readLine()) != null) {
								mCenters.put(line.split("[^0-9E.,\\-]")[0],
										toDoubletArray(line.split("[^0-9E.,\\-]")[1].replaceAll("[^0-9E.,\\-]+", "")));
							}
						} finally {
							br.close();
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String Value = value.toString().split("[; ]+")[1];
			if (!Value.isEmpty()) {
				double[] vArray = toDoubletArray(Value);
				String c = mCenters.keySet().toArray()[0].toString();
				double r = EuDistance(vArray, mCenters.get(c));
				for (String k : mCenters.keySet()) {
					if (r >= EuDistance(vArray, mCenters.get(k))) {
						r = EuDistance(vArray, mCenters.get(k));
						c = k;
					}
				}
				context.write(new Text(c), new Text(Value));
			}
		}
	}

	public static class AverageReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException, NullPointerException {
			int numerator = 0;
			String denominatorString = "";
			double[] denominator = null;
			for (Text t : values) {
				if (!t.toString().isEmpty()) {
					numerator++;
					if (numerator == 1) {
						denominator = ArrayAdd(denominatorString, t.toString());
					} else {
						denominator = ArrayAdd(denominator, toDoubletArray(t.toString()));
					}
				}
			}
			for (int i = 0; i < denominator.length; i++) {
				denominator[i] = Math.round((denominator[i] / numerator) * 100) / 100.0;
			}
			context.write(key, new Text(Arrays.toString(denominator).replaceAll("[^0-9.,.]+", "")));
		}
	}

	public static double EuDistance(double[] x, double[] y) {
		double d = 0;
		for (int i = 0; i < x.length; i++) {
			if (x[i] != 0 || y[i] != 0)
				d = d + Math.pow((x[i] - y[i]), 2.0);
		}
		return Math.sqrt(d);
	}

	public static double[] toDoubletArray(String s) {
		String[] sArray = s.split(",");
		double[] doubleArray = new double[sArray.length];
		for (int i = 0; i < sArray.length; i++) {
			doubleArray[i] = Double.valueOf(sArray[i]);
		}
		return doubleArray;
	}

	public static double[] ArrayAdd(String s1, String s2) {
		if (s1 == "") {
			return toDoubletArray(s2);
		} else {
			double[] doubleArray1 = toDoubletArray(s1);
			double[] doubleArray2 = toDoubletArray(s2);
			for (int i = 0; i < doubleArray1.length; i++) {
				doubleArray1[i] += doubleArray2[i];
			}
			return doubleArray1;
		}
	}

	public static double[] ArrayAdd(double[] d1, double[] d2) {
		for (int i = 0; i < d1.length; i++) {
			d1[i] += d2[i];
		}
		return d1;
	}

	public static void main(String[] args) throws Exception {
		int counter = 1;
		Configuration conf = new Configuration();
		JobConf jobconf = new JobConf(conf);
		jobconf.setNumMapTasks(100);
		String[] Args = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (Args.length != 4) {
			System.err.println("Usage: <input> <center> <output> <iternumber>");
			System.exit(2);
		}
		Path input = new Path(Args[0]);
		Path center = new Path(Args[1]);
		Path inter = new Path(Args[1] + "_int");
		Path output = new Path(Args[2]);
		int iterNumber = Integer.parseInt(Args[3]);
		System.out.println("The " + counter + " iteration start...");
		Job job = new Job(conf, "kmenasJob");
		job.setJarByClass(Kmeans.class);
		DistributedCache.addCacheFile(center.toUri(), job.getConfiguration());
		FileSystem hdfs = input.getFileSystem(conf);
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}
		job.setMapperClass(PartitionMapper.class);
		job.setReducerClass(AverageReducer.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int code = job.waitForCompletion(true) ? 0 : 1;
		while (counter < iterNumber) {
			if (code == 0) {
				++counter;
				System.out.println("The " + counter + " iteration start...");
				if (hdfs.exists(output)) {
					hdfs.rename(output, inter);
				}
				Job secondaryJob = new Job(conf, "kmenasJob");
				secondaryJob.setJarByClass(Kmeans.class);
				FileSystem fs = inter.getFileSystem(conf);
				Path pathPattern = new Path(inter, "part-r-[0-9]*");
				FileStatus[] list = fs.globStatus(pathPattern);
				for (FileStatus status : list) {
					DistributedCache.addCacheFile(status.getPath().toUri(), secondaryJob.getConfiguration());
				}
				if (hdfs.exists(output)) {
					hdfs.delete(output, true);
				}
				secondaryJob.setMapperClass(PartitionMapper.class);
				secondaryJob.setReducerClass(AverageReducer.class);
				FileInputFormat.addInputPath(secondaryJob, input);
				FileOutputFormat.setOutputPath(secondaryJob, output);
				secondaryJob.setOutputKeyClass(Text.class);
				secondaryJob.setOutputValueClass(Text.class);
				code = secondaryJob.waitForCompletion(true) ? 0 : 1;
			}
			hdfs.delete(inter, true);
		}
		if (hdfs.exists(inter)) {
			hdfs.delete(inter, true);
		}
		System.exit(code);
	}
}