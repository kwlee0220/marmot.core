package marmot.io.mapreduce.spcluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.HdfsPath;
import marmot.io.RecordWritable;
import marmot.io.geo.cluster.SpatialCluster;
import marmot.io.geo.cluster.SpatialClusterInfo;
import marmot.io.mapreduce.spcluster.SpatialClusterInputFileFormat.Parameters;
import marmot.support.DefaultRecord;
import utils.StopWatch;
import utils.io.IOUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class SpatialClusterRecordReader extends RecordReader<NullWritable, RecordWritable> {
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialClusterRecordReader.class);
	private static final NullWritable NULL = NullWritable.get();
	
	private RecordSet m_rset;
	private RecordWritable m_next;
	private Record m_record;
	private StopWatch m_watch;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		SpatialClusterFileSplit scfSplit = (SpatialClusterFileSplit)split;
		
		m_watch = StopWatch.start();

		Parameters params = SpatialClusterInputFileFormat.getParameters(conf);
		
		HdfsPath path = HdfsPath.of(conf, scfSplit.getPath());
		SpatialClusterInfo scInfo = scfSplit.getSpatialClusterInfo();
		
		FStream<Record> strm;
		if ( params.m_range != null ) {
			SpatialCluster cluster = new SpatialCluster(path, scInfo, params.m_gschema);
			strm = cluster.queryRecord(params.m_range, true);
		}
		else {
			strm = SpatialCluster.readAll(path, scInfo, params.m_gschema.getRecordSchema())
								.take(scInfo.recordCount() - scInfo.duplicateCount());
		}
		m_rset = RecordSet.from(params.m_gschema.getRecordSchema(), strm);
		
		RecordSchema schema = params.m_gschema.getRecordSchema();
		m_next = RecordWritable.from(schema);
		m_record = DefaultRecord.of(schema);
		
		s_logger.info("open {}", this);
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(m_rset);
		m_watch.stop();
		
		if ( s_logger.isInfoEnabled() ) {
			s_logger.info("close {}, elapsed={}", this, m_watch.getElapsedMillisString());
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if ( m_rset.next(m_record) ) {
			m_next.loadFrom(m_record);
			return true;
		}
		
		return false;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NULL;
	}

	@Override
	public RecordWritable getCurrentValue() throws IOException, InterruptedException {
		return m_next;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0f;
	}
	
	@Override
	public String toString() {
		return m_rset.toString();
	}
}