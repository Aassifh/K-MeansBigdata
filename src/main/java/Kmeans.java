import java.io.*;

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
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Kmeans {
	private final static Map<Integer, List<Double>> Clusters = new HashMap<Integer, List<Double>>();
	private final static List<Double> LastCentroids = new ArrayList<Double>();
	private final static List<Double> newCentroids = new ArrayList<Double>();
	public static class KmeansMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
		
		double min = Double.MAX_VALUE;

		@Override
		protected void setup(Mapper<Object, Text, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// il faut recalculer les centroids
			LastCentroids.clear();
			Configuration conf = context.getConfiguration();
			Path centroids = new Path("/usr/local/hadoop-2.7.3/relai");
			System.out.println("hello " + conf.get("centroid.path"));
			FileSystem fs = FileSystem.get(conf);

			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroids)));

				String line = br.readLine();
				while (line != null) {

					LastCentroids.add(Double.parseDouble(line));
					System.out.println("we added -->" + line);
					line = br.readLine();
				}
			} catch (Exception e) {

			}

			// Centroids.add(-900000000.0);
			// Centroids.add(-900000000.0);
			// Centroids.add(-891154290.0);
			// nouveau centroid = ancien /nb_points;
			// condition d'arret de l'algorithme est :
			// pas de distance entre les anciens centroid et les nouveaux ==>
			// pas de mouvement

			super.setup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split(",");
			int k = context.getConfiguration().getInt("cluster", 3);
			// checl later wont get the data sent from the job
			HashMap<Integer, Double> clustering = new HashMap<Integer, Double>();

			int colonne = context.getConfiguration().getInt("colonne", 0);
			double distance = 0;
			double plus_proche = 0;
			System.out.println("k-->" + k);
			System.out.println("colonne-->" + colonne);
			System.out.println("min-->" + min);
			for (int i = 0; i < k; i++) {
				// probleme du meme point
				if (Double.parseDouble(splits[colonne]) != LastCentroids.get(i))
					distance = Double.parseDouble(splits[colonne]) - LastCentroids.get(i);
				System.out.println("dis --> " + distance);
				System.out.println("min-->" + min);
				if (distance < min) {

					min = distance;
					System.out.println("min-->" + min);
					if (clustering.containsValue(Double.parseDouble(splits[colonne]))) {
						clustering.values().remove(Double.parseDouble(splits[colonne]));
						System.out.println("here we are ");

						clustering.put(i, Double.parseDouble(splits[colonne]));
					} // ici on affecte les eléméent au cluster
						// mais il faut recalculer le centroids
						// l'indice définit le cluster auquel le point
						// appartient

				} else
					clustering.put(i, Double.parseDouble(splits[colonne]));

			}
			double sum = 0;
			Iterator it = clustering.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				sum += (double) pair.getValue();

				context.write(new IntWritable((int) pair.getKey()), new DoubleWritable((double) pair.getValue()));
			}

			min = Double.MAX_VALUE;
		}
	}

	public static class KmeansReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		

		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			List<Double> cluster_points = new ArrayList<Double>();
			for (DoubleWritable doubleWritable : values) {
				cluster_points.add(doubleWritable.get());
				context.write(key, doubleWritable);
			}
			Clusters.put(key.get(), cluster_points);

		}

		@Override
		protected void cleanup(Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			newCentroids.clear();
			Configuration configuration = context.getConfiguration();
			FileSystem hdfs = FileSystem.get(configuration);
			Path file = new Path("/usr/local/hadoop-2.7.3/relai");
			if (hdfs.exists(file)) {
				hdfs.delete(file, true);
			}

			OutputStream os = hdfs.create(file);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			int k = context.getConfiguration().getInt("cluster", 3);
			for (Map.Entry<Integer, List<Double>> entry : Clusters.entrySet()) {
				Integer key = entry.getKey();
				List<Double> value = entry.getValue();
				double sum = 0;
				for (Double double1 : value) {
					sum += double1;
				}

				br.write(String.valueOf(sum / value.size())+"\n");
				newCentroids.add(sum / value.size());

			}

			br.close();
			hdfs.close();
			super.cleanup(context);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Kmeans");
		conf.setInt("cluster", Integer.valueOf(args[1]));
		conf.set("colonne", args[2]);
		// Path centroid = new Path("/usr/local/hadoop-2.7.3/relai");
		// conf.set("centroid.path", centroid.toString());
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
		// ici ou on va faire la boucle de depth et sa conditon d'arret 
		// le while finish sera ici 
		boolean finish = false ;
		while(!finish){
			Configuration conf_inside = new Configuration();
			Job job_inside = Job.getInstance(conf, "Kmeans");
			conf_inside.setInt("cluster", Integer.valueOf(args[1]));
			conf_inside.set("colonne", args[2]);
			job_inside.setInputFormatClass(TextInputFormat.class);
			job_inside.setOutputFormatClass(TextOutputFormat.class);
			job_inside.setNumReduceTasks(1);
			job_inside.setJarByClass(Kmeans.class);
			job_inside.setMapperClass(KmeansMapper.class);
			job_inside.setMapOutputKeyClass(IntWritable.class);
			job_inside.setMapOutputValueClass(DoubleWritable.class);
			job_inside.setReducerClass(KmeansReducer.class);
			job_inside.setOutputKeyClass(IntWritable.class);
			job_inside.setOutputValueClass(DoubleWritable.class);
			double distance =0;
			for(int i =0; i<conf.getInt("k", 3);i++)
				distance+=LastCentroids.get(i)-newCentroids.get(i);
				if (distance ==0)
					finish=true;
				//FileOutputFormat.setOutputPath(job, new Path("nomdichier._means_k19"));
				job_inside.waitForCompletion(true);
		}
//		
		
		// FileInputFormat.addInputPath(job, centroid);
		// MultipleInputs.addInputPath(job, new
		// Path(args[0]),TextInputFormat.class,KmeansMapper.class);
		FileOutputFormat.setOutputPath(job, new Path("nomdichier._means_k19"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
