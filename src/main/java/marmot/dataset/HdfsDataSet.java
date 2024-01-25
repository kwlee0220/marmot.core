package marmot.dataset;

import marmot.io.HdfsPath;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface HdfsDataSet<SCAN> extends DataSetX<SCAN> {
	public HdfsPath getHdfsPath();
	public long getBlockSize();
}
