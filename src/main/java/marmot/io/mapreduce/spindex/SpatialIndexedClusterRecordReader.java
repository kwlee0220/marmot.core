package marmot.io.mapreduce.spindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotInternalException;
import marmot.Record;
import marmot.io.HdfsPath;
import marmot.io.RecordWritable;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.geo.index.SpatialIndexedCluster;
import marmot.io.mapreduce.spindex.SpatialIndexedFileInputFormat.Parameters;
import marmot.optor.support.colexpr.ColumnSelectionException;
import marmot.support.EnvelopeTaggedRecord;
import utils.StopWatch;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class SpatialIndexedClusterRecordReader extends RecordReader<NullWritable, RecordWritable> {
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialIndexedClusterRecordReader.class);
	private static final NullWritable NULL = NullWritable.get();
	
	private GlobalIndexEntry m_entry;
	private FStream<Record> m_records;
	private RecordWritable m_current;
	private int m_idx;
	private StopWatch m_watch;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		SpatialIndexedFileSplit cfsplit = (SpatialIndexedFileSplit)split;
		
		m_watch = StopWatch.start();
		
		Parameters params;
		try {
			params = SpatialIndexedFileInputFormat.getParameters(conf);
		}
		catch ( ColumnSelectionException e ) {
			throw new MarmotInternalException("fails to initialize ClusterRecordReader", e);
		}
		
		// get target index entry
		m_entry = cfsplit.getIndexEntry();

		// open the target spatial cluster
		HdfsPath clusterPath = HdfsPath.of(conf, cfsplit.getPath());
		SpatialIndexedCluster cluster = SpatialIndexedCluster.load(clusterPath, cfsplit.getStart(),
																	cfsplit.getLength());
		
		m_records = (params.m_range != null)
					? cluster.queryRecord(params.m_range, true)
					: cluster.read(true).map(EnvelopeTaggedRecord::getRecord);
		m_idx = 0;
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info(String.format("open: SpatialCluster[quadkey=%s%s]: from=%s:%s",
										m_entry.quadKey(),
										params.m_range != null ? ",range=" +  params.m_range : "",
										cfsplit.getPath(), cfsplit.getStart()));
		}
	}

	@Override
	public void close() throws IOException {
		m_watch.stop();
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info(String.format("close: SpatialCluster[key=%s,count=%d], elapsed=%s",
										m_entry.quadKey(), m_idx, m_watch.getElapsedMillisString()));
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return m_records.next()
						.ifPresent(r -> {
							m_current = RecordWritable.from(r);
							++m_idx;
						})
						.isPresent();
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NULL;
	}

	@Override
	public RecordWritable getCurrentValue() throws IOException, InterruptedException {
		return m_current;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}
}