import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
		private final List<Point> initialCentroids = new ArrayList<Point>();
		double min = Double.MAX_VALUE;

		@Override
		protected void setup(Mapper<Object, Text, Point, Point>.Context context)
				throws IOException, InterruptedException {
			// creer un fichier dont on va stocker les centroid
			// on va lire à chaque fois les centroids mais la premier on vas
			// prendre un par defaut selon
			// le k

			super.setup(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split(",");

			int k = context.getConfiguration().getInt("k", 1);// define the
																// grouping
			// add initial centroids
			for (int i = 0; i < k; i++)
				initialCentroids.add(new Point(Double.valueOf(splits[0]), Double.valueOf(splits[1])));
			// calculate distance with other points
			double distance;
			Point nearest = null;
			for (int i = 0; i < k; i++) {
				distance = Math.sqrt(Math.pow((Double.valueOf(splits[0]) - initialCentroids.get(i).point.getX()), 2)
						+ (Math.pow(Double.valueOf(splits[1]) - initialCentroids.get(i).point.getY(), 2)));
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
			 Centroids.add(newCentroid);
			for (Point point : ListPoint) {
				// context.write(arg0, point);
			}
			// context.write(key, result);
		}

		@Override
		protected void cleanup(Reducer<Point, Point, Point, Point>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// on va stocker les centroids dans le fichier relai qui va étre lu
			// dans la partie setup

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
