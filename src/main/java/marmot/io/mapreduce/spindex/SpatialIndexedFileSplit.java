package marmot.io.mapreduce.spindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import marmot.io.geo.index.GlobalIndexEntry;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexedFileSplit extends FileSplit {
	private static final String[] EMPTY_HOSTS = new String[0];
	
	private GlobalIndexEntry m_info;
	
	public SpatialIndexedFileSplit() { }
	public SpatialIndexedFileSplit(Path file, long start, long length, GlobalIndexEntry info) {
		super(file, start, length, EMPTY_HOSTS);
		
		m_info = info;
	}
	
	public GlobalIndexEntry getIndexEntry() {
		return m_info;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		m_info = GlobalIndexEntry.deserialize(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		m_info.serialize(out);
	}
}