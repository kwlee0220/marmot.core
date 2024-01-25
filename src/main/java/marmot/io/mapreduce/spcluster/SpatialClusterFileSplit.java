package marmot.io.mapreduce.spcluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import marmot.io.geo.cluster.SpatialClusterInfo;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialClusterFileSplit extends FileSplit {
	private static final String[] EMPTY_HOSTS = new String[0];
	
	private SpatialClusterInfo m_scInfo;		// 공간 클러스터 등록정보
	
	public SpatialClusterFileSplit() { }
	public SpatialClusterFileSplit(Path path, SpatialClusterInfo scInfo) {
		super(path, scInfo.start(), scInfo.length(), EMPTY_HOSTS);
		
		m_scInfo = scInfo;
	}
	
	public SpatialClusterInfo getSpatialClusterInfo() {
		return m_scInfo;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		
		m_scInfo = SpatialClusterInfo.deserialize(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		
		m_scInfo.serialize(out);
	}
}