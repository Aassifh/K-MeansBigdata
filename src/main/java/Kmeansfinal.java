import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;




public class Kmeansfinal {
	public  static class KMapper extends Mapper<Object ,Text, NullWritable, Text> {
		int k = 0;
		String  [] nbColonnes = null;
		Path CentroidsPath = null;
		
		
		
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			k = conf.getInt("NbCluster", 10);
			nbColonnes = String.valueOf(conf.get("DimColumns","0")).split(",");
		}
		
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split(",");
			List<Double> points = new ArrayList<Double>();
			
			
			int indice = 0;
			double distance=0.0;
			double min = Double.MAX_VALUE;
	
			double nearest = 0.0;
			for (int i =0; i<nbColonnes.length;i++){
				points.add(Double.valueOf(splits[Integer.valueOf(nbColonnes[i])]));
				
			}
			System.out.println("points  from file>>"+points);
			for (int i = 0; i < k; i++) {
				List<Double> points1 = new ArrayList<Double>();

				for (int j =0; j<nbColonnes.length;j++){
					
					points1.add(context.getConfiguration().getDouble("Center"+i+"_"+nbColonnes[j], 0.0));
				}

				distance = (new PointWritable(points)).distance(points1);

				if (distance < min  )  {
					
					min = distance;
					//nearest = context.getConfiguration().getDouble("Center" + i, 0.0);
					indice=i;
					
				

					
				}
			
				System.out.println("Centers coords in iteration "+i+" >>"+points1);
			}
			System.out.println("indice of the nearest >>"+indice);
			
			context.write(NullWritable.get(),new Text(value.toString()+","+indice) );
		
		}
		
		
		public void cleanup(Context context) {
			
		}
	}
	public  static class KReducer extends Reducer<NullWritable,Text ,NullWritable ,Text > {
		
		public void setup(Context context) {
			
		}
		
		
		
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(key, text);
			}
		}
		
		
		public void cleanup(Context context) {
			
		}

}
}
