import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Point implements WritableComparable<Point> {
	
	public Point2D.Double getPoint() {
		return point;
	}

	public void setPoint(Point2D.Double point) {
		this.point = point;
	}



	public Point2D.Double point;
	
	public Point(){
		super();
	}
	
	public Point(double x, double y) {
		this.point= new Point2D.Double(x, y);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		point.x=in.readDouble();
		point.y= in.readDouble();
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(point.x);
		out.writeDouble(point.y);
	}

	

	public int compareTo(Point o) {
		// TODO Auto-generated method stub
		return 0;
	}
	public String toString(){
		return "x :"+this.point.getX()+",y="+this.point.getY()+"\n";
	}
}
