import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KmeansNew {
	public static class KMeansNewMapper extends Mapper<Object, Text, DoubleWritable, PointWritable> {
		private List<Double> Centroids = new ArrayList<Double>();

		int k = 0;
		int nbColonnes = 0;
		Path CentroidsPath = null;

		protected void setup(Context context) throws IOException, InterruptedException {
			// creer un fichier dont on va stocker les centroid
			// // on va lire à chaque fois les centroids mais la premier on vas
			// // prendre un par defaut selon
			// // le k
			Configuration conf = context.getConfiguration();
			k = conf.getInt("NbCluster", 10);
			nbColonnes = conf.getInt("NbColonnes", 0);
			// System.out.println("K>>" + k);
			//
			// System.out.println("nbColonnes >> " + nbColonnes);
			//
			// // à modifier
			// System.out.println(" i should have it here" +
			// conf.get("Centroid"));
			// Centroids=setCentroids(k, conf.get("Centroids"), conf);

			super.setup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split(",");

			// System.out.println("K inside mapper>>" + k);
			// System.out.println("nbColonnes inside mapper >> " + nbColonnes);
			int indice = 0;
			double distance;
			double min = Double.MAX_VALUE;
			//Double points [] = new Double[nbColonnes];
			DoubleWritable nearest = null;
			for (int i = 0; i < k; i++) {
				// System.out.println("measuring distance with
				// >>"+context.getConfiguration().getDouble("Center"+i, 0.0));
				// System.out.println("with"+
				// Double.valueOf(splits[nbColonnes]));
				distance = Double.valueOf(splits[nbColonnes]) - context.getConfiguration().getDouble("Center" + i, 0.0);
				// System.out.println("the distance >>"+distance);
				if (distance < min) {
					min = distance;
					nearest = new DoubleWritable(context.getConfiguration().getDouble("Center" + i, 0.0));
					indice=i ;
				}
				// à voir

			}

			context.write(nearest, new PointWritable(indice,Double.valueOf(splits[nbColonnes])));
		}

		public void cleanup(Mapper<Object, Text, DoubleWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {

			super.cleanup(context);

		}
	}

	public static class KMeansNewReducer extends Reducer<DoubleWritable, PointWritable, IntWritable, PointWritable> {
		protected void setup(Reducer<DoubleWritable, PointWritable, IntWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {

		}

		public void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {
			// here we will receive a list of point which their key is the
			// nearest center
			// by that point we can identify to which cluster it belongs
			// now we have to calculate the centroids
			double sum = 0.0;
			int nbelem = 0;
			for (PointWritable value : values) {
				sum += value.Point;
				nbelem++;
				context.write(key, value);
			}

			 context.getConfiguration().setDouble("newCenter" + key, sum / nbelem);

			context.getCounter("newCenters", key.toString()).setValue(Double.doubleToLongBits(sum / nbelem));

			System.out.println("newCenter is set with the value >>" + sum / nbelem);

		}

		public void cleanup(Reducer<DoubleWritable, PointWritable, IntWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {

		}
	}

	public static List<Double> setCentroids(int NbCluster, String Input, Configuration conf, int dimension)
			throws IOException {
		// more to come : dimension management !!!
		System.out.println("setCentroids is called");
		List<Double> Centroids = new ArrayList<Double>();

		FileSystem fs = FileSystem.get(conf);
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Input))));

			String line = br.readLine();
			System.out.println("we are reading" + line);

			while (line != null) {
				System.out.println("inside boucle" + line);
				String[] splits = line.split(",");
				Centroids.add(Double.parseDouble(splits[0]));
				System.out.println("we added -->" + splits[0]);
				if (Centroids.size() == NbCluster)
					break;
				line = br.readLine();
			}
		} catch (Exception e) {

		}
		System.out.println("inside the function" + Centroids);
		return Centroids;

	}

	public static boolean iterationChecking(List<Double> oldCenters, List<Double> newCenters, int NbCluster) {
		// more to come if we have large dimensions
		double distance = 0.0;
		for (int i = 0; i < NbCluster; i++) {
			// not sure
			distance += oldCenters.get(i) - newCenters.get(i);
		}

		return distance == 0.0;

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		List<Double> Centroids = new ArrayList<Double>();
		Job job = null;
		conf.set("Centroid", args[0]);
		conf.setInt("NbCluster", Integer.valueOf(args[2]));
		conf.setInt("NbColonnes", Integer.valueOf(args[3]));
		Centroids = setCentroids(conf.getInt("NbCluster", 1), conf.get("Centroid"), conf, 0);
		for (int i = 0; i < Centroids.size(); i++) {
			conf.setDouble("Center" + i, Centroids.get(i));
		}
		System.out.println("Centroids >>" + Centroids);

		boolean converged = false;
		int nbiteration = 0;
		List<Double> newCenters = new ArrayList<Double>();
		while (!converged) {
			String output = args[1] + "_" + nbiteration + "_" + System.nanoTime();

			job = Job.getInstance(conf, "Kmeans_iteration" + nbiteration);
			job.setNumReduceTasks(1);
			job.setJarByClass(KmeansNew.class);
			job.setMapperClass(KMeansNewMapper.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(PointWritable.class);
			job.setReducerClass(KMeansNewReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));

			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);

			for (int i = 0; i < Centroids.size(); i++) {
			// here the fucking bug the getCounter doesnt work !!
			
			//  essayer de récupérer !! le nouveaux centres !!
			System.out.println(" using counters "
					+ Double.longBitsToDouble(job.getCounters().findCounter("newCenters", String.valueOf(Centroids.get(i))).getValue()));

			newCenters
					.add(Double.longBitsToDouble(job.getCounters().findCounter("newCenters",String.valueOf(Centroids.get(i))).getValue()));
			// put new centers in onld centroids list
			 }
			System.out.println("what the hell is happening  to the number of clusters>>" + conf.getInt("NbCluster", 1));
			converged = iterationChecking(Centroids, newCenters, conf.getInt("NbCluster", 1));
			Centroids = converged ? Centroids : newCenters;
			nbiteration++;
		}

		job = Job.getInstance(conf, "Kmeans");

		job.setNumReduceTasks(1);
		job.setJarByClass(KmeansNew.class);
		job.setMapperClass(KMeansNewMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(KMeansNewReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
