import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.*;

public class Convert2Seq {

	public static void main(String args[]) throws Exception {

		// 轉好的檔案命名為point-vector
		String output = "~/point-vector";
		FileSystem fs = null;
		SequenceFile.Writer writer;
		Configuration conf = new Configuration();
		fs = FileSystem.get(conf);
		Path path = new Path(output);
		writer = new SequenceFile.Writer(fs, conf, path, Text.class, VectorWritable.class);

		// 所有的csv檔都在factory路徑底下
		File folder = new File("~/factory");
		File[] listOfFiles = folder.listFiles();

		// 分批把factor路徑底下的csv轉成name-vector格式之後寫進point-vector裡面
		for (int i = 0; i < listOfFiles.length; i++) {

			String input = listOfFiles[i].toString();

			VectorWritable vec = new VectorWritable();
			try {
				FileReader fr = new FileReader(input);
				BufferedReader br = new BufferedReader(fr);
				String s = null;
				while ((s = br.readLine()) != null) {
					String spl[] = s.split(",");
					String key = spl[0];
					Integer val = 0;
					double[] colvalues = new double[1000];
					for (int k = 1; k < spl.length; k++) {
						colvalues[val] = Double.parseDouble(spl[k]);
						val++;
					}
					NamedVector nmv = new NamedVector(new DenseVector(colvalues), key);
					vec.set(nmv);
					writer.append(new Text(nmv.getName()), vec);
				}
			} catch (Exception e) {
				System.out.println("ERROR: " + e);
			}
		}
		writer.close();
	}
}
