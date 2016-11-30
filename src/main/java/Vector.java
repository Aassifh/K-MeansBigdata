import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Vector implements WritableComparable<Vector> {
	
	public double [] vector;
	
	public Vector(){
		super();
	}
	
	public Vector(double x, double y) {
		this.vector = new double[]{x,y};
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int length = in.readInt();
		vector = new double[length];
		for (int i = 0; i < length; i++) {
			vector[i] = in.readDouble();
		}
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(vector.length);
		for (int i = 0; i < vector.length; i++) {
			out.writeDouble(vector[i]);
		}
	}

	public int compareTo(Vector o) {
		// TODO Auto-generated method stub
		boolean equal=false;
		for (int i = 0; i < vector.length; i++) {
			if(vector[i] == o.vector[i]){
				equal = true;
			}else{
				equal = false;
			}
		}
		return equal?1:0;
	}
}
