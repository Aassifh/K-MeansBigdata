import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable {
	public int nbcluster ; 
	// multi dimension point
	 public Double Point ;
	
	public PointWritable(){
		
	}
	
	public PointWritable(int nbcluster, Double point) {
		
		this.nbcluster = nbcluster;
		Point = point;
	}

	@Override
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		nbcluster= in.readInt();
		Point= in.readDouble();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(nbcluster);
		out.writeDouble(Point);
		
	}

	@Override
	public String toString() {
		return Point+","+nbcluster;
	}

	
}