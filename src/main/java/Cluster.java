import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class Cluster implements WritableComparable<Cluster>{
	
	List <Vector> cluster;

	public Cluster() {
		super();
	}
	
	public Cluster(List<Vector> cluster) {
		this.cluster = cluster;
	}
	
	public void addPoint(Vector v){
		this.cluster.add(v);
	}

	public void removePoint(Vector v){
		this.cluster.remove(this.cluster.indexOf(v));
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int length = in.readInt();
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(cluster.size());
		for (Iterator iterator = cluster.iterator(); iterator.hasNext();) {
			Vector vector = (Vector) iterator.next();
			
		}
		
	}

	public int compareTo(Cluster o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
