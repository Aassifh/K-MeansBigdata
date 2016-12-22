import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
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

	public static class KMeansNewMapper extends Mapper<Object, Text, IntWritable, PointWritable> {
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
//			 System.out.println("measuring distance with >>"+context.getConfiguration().getDouble("Center"+i, 0.0));
//				 System.out.println("with"+
//				 Double.valueOf(splits[nbColonnes]));
				 // should compute the nearest center of a center !!
				
				distance = (new PointWritable(Double.valueOf(splits[nbColonnes]))).distance(new PointWritable(context.getConfiguration().getDouble("Center" + i, 0.0)));
//				 System.out.println("the distance >>"+distance);
				if (distance < min  )  {
					min = distance;
					nearest = context.getConfiguration().getDouble("Center" + i, 0.0);
					indice=i;
				}
				// à voir

			}
			// indice ==> id of the cluster of which the points belongs to
			context.write(new IntWritable(indice), new PointWritable(Double.valueOf(splits[nbColonnes])));
		}

		public void cleanup(Mapper<Object, Text, IntWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {

			super.cleanup(context);

		}
	}
	public static class KmeansNewCombiner extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {
		
		public void reduce(IntWritable key, Iterable<PointWritable> values,Context context) throws IOException, InterruptedException {
			Double sum = 0.0;
			int nbelem = 0;
			//int k =0;
			// here we will receive a list of point which their key is the
						// nearest center
						// by that point we can identify to which cluster it belongs
						// now we have to calculate the centroids
						
						for (PointWritable value : values) {
							sum += value.Point;
							nbelem++;
							
						}
						
						if(sum.equals(0)){
							System.out.println("PAS CONTENT");
						}
							
//						System.out.println("sum >>"+sum);
//						System.out.println("nbeleme >>"+nbelem);
						context.write(key, new PointWritable(sum/nbelem));
		}
	}

	
	public static class KMeansNewReducer extends Reducer<IntWritable, PointWritable, DoubleWritable, PointWritable> {
		int k =0;
		protected void setup(Reducer<IntWritable, PointWritable, DoubleWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {
			
			
			k = context.getConfiguration().getInt("NbCluster", 10);
		}

		public void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			int nbelem = 0;
			for (PointWritable pointWritable : values) {
				
				sum+=pointWritable.Point;
				nbelem++;
				
				context.write(new DoubleWritable(key.get()), pointWritable);
			
			}
			
			//context.getConfiguration().setDouble("newCenters"+key, sum/nbelem);
			
				// il faut envoyer un center pour chaque clé cad chaque cluster 
//			System.out.println("sum >>"+sum);
//			System.out.println("nbeleme >>"+nbelem);
			//for (int i =0; i<k;i++){
			//if( context.getConfiguration().getDouble("Center" + i, 0.0)==key.get())
				context.getCounter("newCenters",""+key+"" ).setValue((long)(sum/nbelem));
			//}
		}

		public void cleanup(Reducer<IntWritable, PointWritable, DoubleWritable, PointWritable>.Context context)
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
			distance = Math.abs(oldCenters.get(i) - newCenters.get(i));
			if (distance > 0.01) {
				return false;
				 
			 }
		}

		return true;

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		List<Double> Centroids = new ArrayList<Double>();
		Job job = null;
		conf.set("Centroid", args[0]);
		conf.setInt("NbCluster", Integer.valueOf(args[2]));
		conf.setInt("NbColonnes", Integer.valueOf(args[3]));
		List<String> Columns = Arrays.asList(Arrays.copyOfRange(args, 3, args.length));
		Centroids = setCentroids(conf.getInt("NbCluster", 1), conf.get("Centroid"), conf, 0);
		
		

		boolean converged = false;
		int nbiteration = 0;
		
		while (!converged) {
			String output = args[1] + "_" + nbiteration + "_" + System.nanoTime();
			System.out.println("Centroids >>" + Centroids);
			
			for (int i = 0; i < conf.getInt("NbCluster", 1); i++) {
				conf.setDouble("Center" + i, Centroids.get(i));
			}
			
			
			job = Job.getInstance(conf, "Kmeans_iteration" + nbiteration);
			job.setNumReduceTasks(1);		
			job.setJarByClass(KmeansNew.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapperClass(KMeansNewMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(PointWritable.class);
			job.setCombinerClass(KmeansNewCombiner.class);
			job.setReducerClass(KMeansNewReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(PointWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));

			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			System.out.println("new centroids after job :");
			for(Double c : Centroids) {
				System.out.println(">>"+c);
			}
			
			List<Double> newCenters = new ArrayList<Double>();
				
			for (int i = 0; i < conf.getInt("NbCluster", 1); i++) {
				Double newCenter2 = (double)job.getCounters().findCounter("newCenters",""+i+"").getValue();
				System.out.println(" here is the counter "+newCenter2);
				// here the fucking bug the getCounter doesnt work !!
				
				//  essayer de récupérer !! le nouveaux centres !!
				
				newCenters.add(newCenter2);
				// put new centers in onld centroids list
			}

			//System.out.println("what the hell is happening  to the number of clusters>>" + conf.getInt("NbCluster", 1));
			
			//System.out.println("size  of  centroids >>"+ Centroids.size());
			
			
			
			System.out.println("new centroids :");
			for(Double c : newCenters) {
				System.out.println(">>"+c);
			}
			converged = iterationChecking(Centroids, newCenters, conf.getInt("NbCluster", 1));
			System.out.println("SIZE OF NEW CENTROIDS"+newCenters.size());
			Centroids =  newCenters;
			System.out.println("new centroids :");
			for(Double c : Centroids) {
				System.out.println(">>"+c);
			}
			System.out.println("SIZE OF CENTROIDS"+Centroids.size());
			nbiteration++;
		}

//		job = Job.getInstance(conf, "Kmeans");
//
//		job.setNumReduceTasks(1);
//		job.setJarByClass(KmeansNew.class);
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
//		job.setMapperClass(KMeansNewMapper.class);
//		job.setMapOutputKeyClass(DoubleWritable.class);
//		job.setMapOutputValueClass(PointWritable.class);
//		job.setCombinerClass(KmeansNewCombiner.class);
//		job.setReducerClass(KMeansNewReducer.class);
//		job.setOutputKeyClass(DoubleWritable.class);
//		job.setOutputValueClass(PointWritable.class);
//		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
