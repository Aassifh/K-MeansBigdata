import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;

public class PointOutputFormat extends TextOutputFormat<Point, NullWritable>{

	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RecordWriter<Point, NullWritable> getRecordWriter(FileSystem arg0, JobConf arg1, String arg2,
			Progressable arg3) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	

	
}
