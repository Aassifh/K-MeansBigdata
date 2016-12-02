import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Kmeans {

	public static class KmeansMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		private final List<Double> Centroids = new ArrayList<Double>();
		double min = Double.MAX_VALUE;

		@Override
		protected void setup(Mapper<Object, Text, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			
				Centroids.add(-900000000.0);
				Centroids.add(-900000000.0);
				Centroids.add(-891154290.0);
				
			super.setup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split(",");
			int k = context.getConfiguration().getInt("cluster",3);//checl later wont get the data sent from the job 
			HashMap<Integer, Double> clustering = new HashMap<Integer, Double>();
			double distance = 0;
			int colonne = context.getConfiguration().getInt("colonne", 0);
			double plus_proche=0;
			System.out.print("k-->" + k);
			System.out.println("colonne-->" + colonne);
			
			for (int i = 0; i < k; i++) {
				distance+= Double.parseDouble(splits[colonne]) - Centroids.get(i);
				System.out.println("dis --> "+distance);
			if (distance < min) {
					System.out.println("min-->" + colonne);
					min = distance;
					if (clustering.containsValue(Double.parseDouble(splits[colonne]))) {
						clustering.values().remove(Double.parseDouble(splits[colonne]));
						clustering.put(i, Double.parseDouble(splits[colonne]));
					}
					
				} 
				
			}

			Iterator it = clustering.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				context.write(new IntWritable((int) pair.getKey()), new DoubleWritable((double) pair.getValue()));
			}

		}
	}

	public static class KmeansReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		// private final List<Point> Centroids = new ArrayList<Point>();

		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			for (DoubleWritable doubleWritable : values) {
				context.write(key, doubleWritable);
			}

		}

		@Override
		protected void cleanup(Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// Configuration conf = context.getConfiguration();
			// Path outPath = new Path(conf.get("output"));
			// FileSystem fs = FileSystem.get(conf);
			// fs.delete(outPath, true);
			// try (SequenceFile.Writer writer = SequenceFile.createWriter(fs,
			// context.getConfiguration(), outPath,
			// IntWritable.class, DoubleWritable.class)) {
			// for (Point point : Centroids) {
			// .append(point, new IntWritable(0));
			// }
			// }
			// // TODO Auto-generated method stub
			//
			super.cleanup(context);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Kmeans");
		conf.setInt("cluster",Integer.valueOf(args[1]));
		conf.set("colonne", args[2]);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(KmeansMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(KmeansReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("nomdichier._means_k19"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
