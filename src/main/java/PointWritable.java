import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable {
	
	// multi dimension point
	 public Double Point ;
	public List<Double> Points;
	public PointWritable(){
		
	}
	
	public PointWritable(Double point) {
		
		
		
		Point = point;
	}
	public PointWritable(List<Double> points){
		this.Points=points;
	}

	@Override
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		;
		Point= in.readDouble();
		for (Double double1 : Points) {
			double1=in.readDouble();
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeDouble(Point);
		for (Double double1 : Points) {
			out.writeDouble(double1);
		}
		
	}
	public double distance (PointWritable p){
		 
		double dist =0.0;
		for (int i=0 ; i<this.Points.size() && i < p.Points.size();i++){
			dist+=Math.sqrt(Math.pow(this.Points.get(i)-p.Points.get(i), 2));
		}
		
	return dist;
	}

	@Override
	public String toString() {
		String s ="Points >>> \n";
		for (Double double1 : Points) {
			s+=double1+" ";
		}
		return s ;
	}

	
}