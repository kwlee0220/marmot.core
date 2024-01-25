package marmot.optor.geo.cluster;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.geo.quadtree.LeafNode;
import marmot.io.geo.quadtree.QuadTree;
import marmot.optor.geo.cluster.RecordSizePartition.RecordSize;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.ProgressReportable;
import marmot.type.DataType;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampledQuadSpace extends AbstractRecordSet implements ProgressReportable {
	public static final String COLUMN_QUADKEY = "quad_key";
	public static final String COLUMN_LENGTH = "length";
	public static RecordSchema SCHEMA = RecordSchema.builder()
													.addColumn(COLUMN_QUADKEY, DataType.STRING)
													.addColumn(COLUMN_LENGTH, DataType.LONG)
													.build();
	
	private final RecordSet m_input;
	private final long m_splitSize;
	private FStream<LeafNode<RecordSize,RecordSizePartition>> m_leafNodes;
	private boolean m_isFirst = true;
	
	protected long m_elapsed;
	private boolean m_finalProgressReported = false;

	public SampledQuadSpace(RecordSet input, long splitSize) {
		m_input = input;
		m_splitSize = splitSize;
	}
	
	@Override
	protected void closeInGuard() {
		m_leafNodes.closeQuietly();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return SCHEMA;
	}

	@Override
	public boolean next(Record record) {
		checkNotClosed();
		
		if ( m_isFirst ) {
			buildRecordSizeIndex(m_input);
			m_isFirst = false;
		}
		
		FOption<LeafNode<RecordSize,RecordSizePartition>> oleaf = m_leafNodes.next();
		if ( oleaf.isPresent() ) {
			LeafNode<RecordSize,RecordSizePartition> leaf = oleaf.get();
			
			long size = leaf.getPartition().getTotalSize();
			record.set(0, leaf.getQuadKey());
			record.set(1, size);
			
			if ( s_logger.isInfoEnabled() ) {
				if ( leaf.getValueCount() > 0 ) {
					s_logger.info(String.format("allocated: partition[quad_key=%s, nsamples=%d, block=%s/%s (%.1f%%)]",
												leaf.getQuadKey(), leaf.getValueCount(),
												UnitUtils.toByteSizeString(size, "mb", "%.1f"),
												UnitUtils.toByteSizeString(m_splitSize, "mb", "%.1f"),
												(size*100.0)/m_splitSize));
				}
			}
			
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("SplitQuadSpace[split_size=%s]",
								UnitUtils.toByteSizeString(m_splitSize));
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if ( !isClosed() || !m_finalProgressReported ) {
			if ( m_input instanceof ProgressReportable ) {
				((ProgressReportable)m_input).reportProgress(logger, elapsed);
			}
			
			m_elapsed = elapsed.getElapsedInMillis();
			logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
			
			if ( isClosed() ) {
				m_finalProgressReported = true;
			}
		}
	}
	
	private void buildRecordSizeIndex(RecordSet rset) {
		QuadTree<RecordSize,RecordSizePartition> qtree
			= new QuadTree<>(qkey->new RecordSizePartition(m_splitSize));
		
		Record inputRecord = DefaultRecord.of(rset.getRecordSchema());
		while ( rset.next(inputRecord) ) {
			Envelope mbr = (Envelope)inputRecord.get(0);
			int length = inputRecord.getInt(1);
			qtree.insert(new RecordSize(mbr, length));
		}
		qtree.compact();
		m_input.closeQuietly();
		
		m_leafNodes = qtree.streamLeafNodes()
							.filter(leaf -> leaf.getValueCount() > 0);
	}
}