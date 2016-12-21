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
		//private List<Double> Centroids = new ArrayList<Double>();

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
			double nearest = 0.0;
			for (int i = 0; i < k; i++) {
				// System.out.println("measuring distance with
				// >>"+context.getConfiguration().getDouble("Center"+i, 0.0));
				// System.out.println("with"+
				// Double.valueOf(splits[nbColonnes]));
				distance = Double.valueOf(splits[nbColonnes]) - context.getConfiguration().getDouble("Center" + i, 0.0);
				// System.out.println("the distance >>"+distance);
				if (distance < min) {
					min = distance;
					nearest = context.getConfiguration().getDouble("Center" + i, 0.0);
					indice=i;
				}
				// à voir

			}
			// indice ==> id of the cluster of which the points belongs to
			context.write(new DoubleWritable(nearest), new PointWritable(indice,Double.valueOf(splits[nbColonnes])));
		}

		public void cleanup(Mapper<Object, Text, DoubleWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {

			super.cleanup(context);

		}
	}
	public static class KmeansNewCombiner extends Reducer<DoubleWritable, PointWritable, DoubleWritable, PointWritable> {
		
		public void reduce(DoubleWritable key, Iterable<PointWritable> values,Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			int nbelem = 0;
			int k =0;
			// here we will receive a list of point which their key is the
						// nearest center
						// by that point we can identify to which cluster it belongs
						// now we have to calculate the centroids
						
						for (PointWritable value : values) {
							sum += value.Point;
							nbelem++;
							
						}

						context.write(key, new PointWritable(nbelem,sum));
		}
	}

	
	public static class KMeansNewReducer extends Reducer<DoubleWritable, PointWritable, IntWritable, PointWritable> {
		int k =0;
		double sum = 0.0;
		int nbelem = 0;
		protected void setup(Reducer<DoubleWritable, PointWritable, IntWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {
			
			
			k = context.getConfiguration().getInt("NbCluster", 10);
		}

		public void reduce(DoubleWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {
			for (PointWritable pointWritable : values) {
				
				sum+=pointWritable.Point;
				nbelem+=pointWritable.nbcluster;
				
				context.write(new IntWritable((int)key.get()), pointWritable);
			}
			context.getConfiguration().setDouble("newCenters"+key, sum/nbelem);
			
			for(int i=0;i<k ;i++)
			context.getCounter("newCenters",""+i+"").setValue((long) (sum/nbelem));
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
				if(!Centroids.contains(Double.parseDouble(splits[0])))
					Centroids.add(Double.parseDouble(splits[0]));
				else line = br.readLine();
				System.out.println("we added -->" + splits[0]);
				if (Centroids.size() == NbCluster)
					break;
				
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

		return distance < 0.01;

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
			for( int i =0; i<Centroids.size();i++)
				newCenters.add(conf.getDouble("newCenters"+Centroids.get(i), -1));
		
			
			job = Job.getInstance(conf, "Kmeans_iteration" + nbiteration);
			job.setNumReduceTasks(1);		
			job.setJarByClass(KmeansNew.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapperClass(KMeansNewMapper.class);
			job.setMapOutputKeyClass(DoubleWritable.class);
			job.setMapOutputValueClass(PointWritable.class);
			job.setCombinerClass(KmeansNewCombiner.class);
			job.setReducerClass(KMeansNewReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(PointWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));

			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			for(int i =0; i<conf.getInt("NbCluster", 1); i++)
			System.out.println(" here is th counter "+Double.longBitsToDouble(job.getCounters().findCounter("newCenters",""+i+"").getValue()));
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
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(KMeansNewMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(PointWritable.class);
		job.setCombinerClass(KmeansNewCombiner.class);
		job.setReducerClass(KMeansNewReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PointWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
