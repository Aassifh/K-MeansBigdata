import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Kmeans {

	public static class KmeansMapper extends Mapper<Object, Text, Point, Point> {
		private final List<Point> Centroids = new ArrayList<Point>();
		double min = Double.MAX_VALUE;

		@Override
		protected void setup(Mapper<Object, Text, Point, Point>.Context context)
				throws IOException, InterruptedException {
			// creer un fichier dont on va stocker les centroid
			// on va lire à chaque fois les centroids mais la premier on vas
			// prendre un par defaut selon
			// le k
			Configuration conf = context.getConfiguration();
			Path CentroidsPath = new Path(conf.get("Centroid"));
			FileSystem fs = FileSystem.get(conf);
			try(SequenceFile.Reader reader = new SequenceFile.Reader(fs, CentroidsPath,conf )){
				Point key = new Point();
				IntWritable value = new IntWritable();
				
				while(reader.next(key,value)){
				Centroids.add(key);
				}
				
				
				
			}

			super.setup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split(",");

			int k = context.getConfiguration().getInt("k", 1);// define the
																// grouping
			// add initial centroids
			for (int i = 0; i < k; i++)
				Centroids.add(new Point(Double.valueOf(splits[0]), Double.valueOf(splits[1])));
			// calculate distance with other points
			double distance;
			Point nearest = null;
			for (int i = 0; i < k; i++) {
				distance = Math.sqrt(Math.pow((Double.valueOf(splits[0]) - Centroids.get(i).point.getX()), 2)
						+ (Math.pow(Double.valueOf(splits[1]) - Centroids.get(i).point.getY(), 2)));
				if (distance < min) {
					min = distance;
					nearest = new Point(Double.valueOf(splits[0]), Double.valueOf(splits[1]));

				}

			}

			context.write(nearest, new Point(Double.valueOf(splits[0]), Double.valueOf(splits[1])));
		}
	}

	public static class KmeansReducer extends Reducer<Point, Point, Point, Point> {
		private final List<Point> Centroids = new ArrayList<Point>();

		public void reduce(Point key, Iterable<Point> values, Context context)
				throws IOException, InterruptedException {

			Point newCentroid = null;
			List<Point> ListPoint = new ArrayList<Point>();
			// define a center
			for (Point value : values) {
				ListPoint.add(value);
				if (newCentroid==null) 			
					newCentroid=value;
				else 
					newCentroid=new Point(newCentroid.getPoint().getX()+value.getPoint().getX(),newCentroid.getPoint().getY()+value.getPoint().getY());
			}
			// on ajoute les nouveaux centroids
			//diviser sur les centroids
			 
			newCentroid=new Point(newCentroid.getPoint().getX()/ListPoint.size(), newCentroid.getPoint().getY()/ListPoint.size());
			Centroids.add(newCentroid);
			 
			for (Point point : ListPoint) {
				context.write(newCentroid, point);
			}
			// context.write(key, result);
		}

		@Override
		protected void cleanup(Reducer<Point, Point, Point, Point>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// on va stocker les centroids dans le fichier relai qui va étre lu
			// dans la partie setup
			Configuration conf = context.getConfiguration();
			Path outPath = new Path(conf.get("Centroid"));
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outPath,true);
			try (SequenceFile.Writer writer = SequenceFile.createWriter(fs,context.getConfiguration(),outPath,Point.class,IntWritable.class)){
				for(Point point: Centroids){
					writer.append(point, new IntWritable(0));
				}
			}

			super.cleanup(context);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Kmeans");
		conf.set("k", args[1]);
		job.setNumReduceTasks(1);
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(KmeansMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Point.class);
		job.setReducerClass(KmeansReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
