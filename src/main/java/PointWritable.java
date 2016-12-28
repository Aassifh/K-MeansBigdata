import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable {

	// multi dimension point

	public List<Double> Points;

	public PointWritable() {

	}
	
	public PointWritable(int dim) {
		for(int i=0;i<dim;i++){
			this.Points.add(0.0);
		}
	}
	
	public PointWritable(List<Double> points) {
		this.Points = new ArrayList<Double>();
		Points = points;
	}

	
// c'est déja public je t'explique ^^
	public List<Double>  getPoint() {
		return this.Points;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		List<Double> p = new ArrayList<Double>();
		int size = in.readInt();
		for (int i = 0; i < size; i++)
			p.add(in.readDouble());
		Points = p;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeInt(Points.size());
		for (Double double1 : Points) {
			out.writeDouble(double1);
		}

	}

//	public double distance(PointWritable p) {
//
//		/*double dist = 0.0;
//		for (int i = 0; i < this.Points.size() && i < p.Points.size(); i++) {
//			dist +=Math.pow(this.Points.get(i) - p.Points.get(i), 2);
//		}
//
//		return Math.sqrt(dist);*/
//		//Si jamais tu as une erreur à ta fonction tu le modifie qu'une fois ^^
//		
//		return distance(p.getPoint());
//		mais j'utilise qu'une seule fonction pour le calcul de la distance 
//	}

	public double distance(List<Double> l) {

		double dist = 0.0;
		for (int i = 0; i < this.Points.size() && i < l.size(); i++) {
			dist +=Math.pow(this.Points.get(i) - l.get(i), 2);
		}

		return Math.sqrt(dist);
	}

	@Override
	public String toString() {
		String s = "Points >>> \n";
		for (Double double1 : Points) {
			s += double1 + " ";
		}
		return s;
	}

}