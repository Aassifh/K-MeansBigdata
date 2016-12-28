import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
		String  [] nbColonnes = null;
		Path CentroidsPath = null;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			// creer un fichier dont on va stocker les centroid
			// // on va lire à chaque fois les centroids mais la premier on vas
			// // prendre un par defaut selon
			// // le k
			Configuration conf = context.getConfiguration();
			k = conf.getInt("NbCluster", 10);
			nbColonnes = String.valueOf(conf.get("DimColumns","0")).split(",");
			// conf.set("DimColumns", String.join(",", Columns));
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
//		public static void debugHDFS(String m, int i) {
//			String file = "debug_" + i;
//			try {
//				Path pt = new Path("/debug" + file);
//				FileSystem fs = FileSystem.get(new Configuration());
//				BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
//				// TO append data to a file, use fs.append(Path f)
//					br.write(m);
//					br.write("\r\n"); 
//				
//			
//				br.close();
//			} catch (Exception e) {
//				System.out.println("File not found");
//			}
//
//		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split(",");
			List<Double> points = new ArrayList<Double>();
			
			// System.out.println("K inside mapper>>" + k);
			// System.out.println("nbColonnes inside mapper >> " + nbColonnes);
			int indice = 0;
			double distance=0.0;
			double min = Double.MAX_VALUE;
			//Double points [] = new Double[nbColonnes];
			double nearest = 0.0;
			for (int i =0; i<nbColonnes.length;i++){
				points.add(Double.valueOf(splits[Integer.valueOf(nbColonnes[i])]));
				
			}
			System.out.println("points  from file>>"+points);
			for (int i = 0; i < k; i++) {
				List<Double> points1 = new ArrayList<Double>();
//			 System.out.println("measuring distance with >>"+context.getConfiguration().getDouble("Center"+i, 0.0));
//				 System.out.println("with"+
//				 Double.valueOf(splits[nbColonnes]));
				 // should compute the nearest center of a center !!
				/*
				 * for one dimension we used
				 * distance = (new PointWritable(Double.valueOf(splits[nbColonnes]))).distance(new PointWritable(context.getConfiguration().getDouble("Center" + i, 0.0)));
				 */
				for (int j =0; j<nbColonnes.length;j++){
					//depends on dimension s=5 et 7
					//System.out.println("getting  center  for cluster "+ i+"and coord "+s+">>"+context.getConfiguration().getDouble("Center"+i+"_"+s, 0.0));
					points1.add(context.getConfiguration().getDouble("Center"+i+"_"+nbColonnes[j], 0.0));
				}

				distance = (new PointWritable(points)).distance(points1);
//				 System.out.println("the distance >>"+distance);
				//debugHDFS("je te troll "+min, 5);
				if (distance < min  )  {
					//context.getCounter("debug", "Point"+points+"_Centres_"+points1+"_min_"+min).setValue((long)distance);
					min = distance;
					//nearest = context.getConfiguration().getDouble("Center" + i, 0.0);
					indice=i;
					System.out.println("couocu");
				

					//debugHDFS("J'ai trouvé une meilleure distance "+min, i);
				}
				// à voir
				System.out.println("Centers coords in iteration "+i+" >>"+points1);
			}
			System.out.println("indice of the nearest >>"+indice);
			// indice ==> id of the cluster of which the points belongs to
			context.write(new IntWritable(indice), new PointWritable(points));
		}

		public void cleanup(Mapper<Object, Text, IntWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {

			super.cleanup(context);

		}
	}
//	public static class KmeansNewCombiner extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {
//		String [] nbColonnes=null;
//		protected void setup(Reducer<IntWritable, PointWritable, IntWritable, PointWritable>.Context context)
//				throws IOException, InterruptedException {
//			
//			nbColonnes= String.valueOf(context.getConfiguration().get("DimColumns","0")).split(",");
//		}
//		public void reduce(IntWritable key, Iterable<PointWritable> values,Context context) throws IOException, InterruptedException {
//			
//			Double [] sum =new Double [nbColonnes.length];
//			/*
//			 * initialize /
//			 */
//			//cE QUE TU peux faire, c'est remettre ton nbelem, dans ton pointswritable, pour faire la moyenne dans le réduceur
//			// et tu trava!illes avec une liste au lieu d'un array
//			// attends je t'explique ce que je fais rapidement ok
//			for (int i = 0; i < nbColonnes.length; ++i)
//			{
//				sum[i] = 0.0;
//			}
//
//			int nbelem = 0;
//			//int k =0;
//			// here we will receive a list of point which their key is the
//						// nearest center
//						// by that point we can identify to which cluster it belongs
//						// now we have to calculate the centroids
//				List<Double> points = new ArrayList<Double>();
//						
//				for (PointWritable value : values) {
//						for(int i =0;i<nbColonnes.length;i++){
//							// ici une sommation dans le combiner 
//							sum[i]+=value.Points.get(i);
//						
//						}
//						// avec le calcul des n
//						nbelem++;
//						}
//						
//				for (int i=0; i<nbColonnes.length;i++)
//					{
//					points.add(sum[i]/nbelem);
//						if(sum[i].equals(0.0)){
//							System.out.println("PAS CONTENT");
//						}
//						}
//							
////						System.out.println("sum >>"+sum);
////						System.out.println("nbeleme >>"+nbelem);
//						context.write(key, new PointWritable(points));
//		}
//	}

	
	public static class KMeansNewReducer extends Reducer<IntWritable, PointWritable, DoubleWritable, PointWritable> {
		int k =0;
		String [] nbColonnes =null;
		protected void setup(Reducer<IntWritable, PointWritable, DoubleWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {
			
			nbColonnes= String.valueOf(context.getConfiguration().get("DimColumns","0")).split(",");
			k = context.getConfiguration().getInt("NbCluster", 10);
		}

		public void reduce(IntWritable key, Iterable<PointWritable> values, Context context)
				throws IOException, InterruptedException {
			Double [] sum =new Double [nbColonnes.length];
			/*
			 * initialize /
			 */
			for (int i = 0; i < nbColonnes.length; ++i)
			{
				sum[i] = 0.0;
			}
			int nbelem = 0;
			//int k =0;
			// here we will receive a list of point which their key is the
						// nearest center
						// by that point we can identify to which cluster it belongs
						// now we have to calculate the centroids
				List<Double> points = new ArrayList<Double>();
						
				for (PointWritable value : values) {
						for(int i =0;i<nbColonnes.length;i++){
							sum[i]+=value.Points.get(i);
						
						}
						nbelem++;
						// ne sert à rien pour le moment 
						context.write(new DoubleWritable(key.get()), value);
				}
						
				for (int i=0; i<nbColonnes.length;i++){
					context.getCounter("newCenters",""+key+"_"+i ).setValue((long)((sum[i]/nbelem)));;
						if(sum[i].equals(0.0)){
							System.out.println("PAS CONTENT");
						}
				}
			//
			//context.getConfiguration().setDouble("newCenters"+key, sum/nbelem);
			
				// il faut envoyer un center pour chaque clé cad chaque cluster 
//			System.out.println("sum >>"+sum);
//			System.out.println("nbeleme >>"+nbelem);
			//for (int i =0; i<k;i++){
			//if( context.getConfiguration().getDouble("Center" + i, 0.0)==key.get())
				
			//}
		}

		public void cleanup(Reducer<IntWritable, PointWritable, DoubleWritable, PointWritable>.Context context)
				throws IOException, InterruptedException {
			
		
			
				
		}
	}

	public static List<List<Double>> setCentroids(int NbCluster, String Input, Configuration conf, List<String> Columns)
			throws IOException {
		// more to come : dimension management !!!
		System.out.println("setCentroids is called");
		List<List<Double>> Centroids = new ArrayList<List<Double>>();
	
		FileSystem fs = FileSystem.get(conf);
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(Input))));

			String line = br.readLine();
			System.out.println("we are reading" + line);

			while (line != null) {
				System.out.println("inside boucle" + line);
				String[] splits = line.split(",");
				
					List<Double> center = new ArrayList<Double>();
					for (int j =0 ;j<Columns.size();j++)
					center.add(Double.valueOf(splits[Integer.valueOf(Columns.get(j))]));
					if(!Centroids.contains(center))
					Centroids.add(center);
					
				
				 line = br.readLine();
				
				if (Centroids.size() == NbCluster)
					break;
				
			}
		} catch (Exception e) {
			 e.printStackTrace();
		}
		System.out.println("inside the function >>" + Centroids);
		return Centroids;

	}

	public static boolean iterationChecking(List<List<Double>> oldCenters, List<List<Double>> newCenters, int NbCluster, int dimension) {
		// more to come if we have large dimensions
		double distance = 0.0;
		
		for (int i = 0; i < NbCluster; i++) {
			// not sure
			for (int j =0;j<dimension;j++)
			distance += Math.pow(oldCenters.get(i).get(j) - newCenters.get(i).get(j),2);
			if (Math.sqrt(distance) > 0.01) {
				return false;
				 
			 }
		}

		return true;

	}

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		List<List<Double>> Centroids = new ArrayList<List<Double>>();
		Job job = null;
		conf.set("Centroid", args[0]);
		conf.setInt("NbCluster", Integer.valueOf(args[2]));
		conf.setInt("NbColonnes", Integer.valueOf(args[3]));
		List<String> Columns = Arrays.asList(Arrays.copyOfRange(args, 3, args.length));
		System.out.println("Clumns >>"+String.join(",", Columns));
		conf.set("DimColumns", String.join(",", Columns));
		System.out.println(Columns.size());
		Centroids = setCentroids(conf.getInt("NbCluster", 1), conf.get("Centroid"), conf, Columns);
		
		

		boolean converged = false;
		int nbiteration = 0;
		
		while (!converged) {
			String output = args[1] + "_" + nbiteration + "_" + System.nanoTime();
			System.out.println("Centroids >>" + Centroids);
			
			for (int i = 0; i < conf.getInt("NbCluster", 1); i++) {
				for (int j=0; j<Columns.size();j++){
		
					System.out.println("setting  with cluster "+i+" coord"+Columns.get(j)+">>"+Centroids.get(i).get(Integer.valueOf(j)));
					conf.setDouble("Center" + i +"_"+ Columns.get(j) , Centroids.get(i).get(Integer.valueOf(j)));
				}
				
				
			}
			
			
			job = Job.getInstance(conf, "Kmeans_iteration" + nbiteration);
			job.setNumReduceTasks(1);		
			job.setJarByClass(KmeansNew.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapperClass(KMeansNewMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(PointWritable.class);
			//job.setCombinerClass(KmeansNewCombiner.class);
			job.setReducerClass(KMeansNewReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(PointWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));

			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			System.out.println("new centroids after job :");
//			for(Double c : Centroids) {
//				System.out.println(">>"+c);
//			}
//			
			List<List<Double>> newCenters = new ArrayList<List<Double>>();
				
			for (int i = 0; i < conf.getInt("NbCluster", 1); i++) {
				List<Double> coords = new ArrayList<Double>();
				for (int j=0 ;j<Columns.size();j++){
				System.out.println("Counter for newCenter "+i +"_"+j+">>"+ (double)job.getCounters().findCounter("newCenters",""+i+"_"+j).getValue());
				coords.add((double)job.getCounters().findCounter("newCenters",""+i+"_"+j).getValue());
				}
				System.out.println(" here is the counter coords >> "+coords);
				
				// here the fucking bug the getCounter doesnt work !!
				
				//  essayer de récupérer !! le nouveaux centres !!
				
				newCenters.add(coords);
				// put new centers in onld centroids list
			}

			//System.out.println("what the hell is happening  to the number of clusters>>" + conf.getInt("NbCluster", 1));
			
			//System.out.println("size  of  centroids >>"+ Centroids.size());
			
			
			
			System.out.println("new centroids :");
//			for(Double c : newCenters) {
//				System.out.println(">>"+c);
//			}
			converged = iterationChecking(Centroids, newCenters, conf.getInt("NbCluster", 1),Columns.size());
			System.out.println("SIZE OF NEW CENTROIDS"+newCenters.size());
			Centroids =  newCenters;
			System.out.println("new centroids :");
//			for(Double c : Centroids) {
//				System.out.println(">>"+c);
//			}
			System.out.println("SIZE OF CENTROIDS"+Centroids.size());
			
			nbiteration++;
			
		}

		job = Job.getInstance(conf, "Kmeans");

		job.setNumReduceTasks(1);
		job.setJarByClass(KmeansNew.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(Kmeansfinal.KMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		//job.setCombinerClass(KmeansNewCombiner.class);
		job.setReducerClass(Kmeansfinal.KReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//String MainPath = "vigo:9000";
		job.waitForCompletion(true);
		FileSystem fs = FileSystem.get(conf);
		FileUtil.copyMerge(fs, new Path(args[1]), fs,new Path(args[1]+".csv"), false, conf, null);

		//System.exit( ? 0 : 1);
		
	

	}

}
