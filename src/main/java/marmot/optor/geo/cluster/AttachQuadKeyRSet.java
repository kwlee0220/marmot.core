package marmot.optor.geo.cluster;

import java.util.List;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.optor.support.QuadKeyBinder;
import marmot.optor.support.QuadKeyBinder.QuadKeyBinding;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.ProgressReportable;
import marmot.type.DataType;
import utils.StopWatch;
import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AttachQuadKeyRSet extends AbstractRecordSet implements ProgressReportable {
	public static final String COL_QUAD_KEY = Constants.COL_QUAD_KEY;
	public static final String COL_MBR = Constants.COL_MBR;
	public static final String OUTLIER_QUADKEY = Constants.QUADKEY_OUTLIER;
	private static final Envelope EMPTY_BOUNDS = new Envelope();

	private final RecordSet m_input;
	private final GeometryColumnInfo m_gcInfo;
	private final List<String> m_qkeys;
	@Nullable private final Envelope m_validRange;
	
	private final RecordSchema m_outSchema;
	private final int m_geomIdx;
	private final int m_quadKeyColIdx;
	private final int m_mbrColIdx;
	@Nullable private final CoordinateTransform m_trans;

	private final FStream<Record> m_output;
	private final QuadKeyBinder m_binder;
	private final boolean m_bindOutlier;
	private final boolean m_bindOnlyToOwner;	// 주어진 공간정보가 포함되는 모든 quad-key를 포함시킬지
												// 아니면 owner quad-key만 포함시킬 지 결정 
	
	protected long m_elapsed;
	private boolean m_finalProgressReported = false;
	
	public AttachQuadKeyRSet(RecordSet input, GeometryColumnInfo gcInfo, List<String> qkeys,
								@Nullable Envelope validRange, boolean bindOutlier,
								boolean bindOnlyToOwner) {
		Utilities.checkNotNullArgument(gcInfo, "GeometryColumnInfo");
		Utilities.checkNotNullArgument(qkeys, "QueryKey list");
		Utilities.checkArgument(qkeys.size() > 0, "empty queryKey list");
		
		m_input = input;
		m_gcInfo = gcInfo;
		m_qkeys = qkeys;
		m_validRange = validRange;
		
		RecordSchema inSchema = input.getRecordSchema();
		m_outSchema = calcOutputRecordSchema(inSchema);
		m_geomIdx = inSchema.getColumn(gcInfo.name()).ordinal();
		m_quadKeyColIdx = m_outSchema.getColumn(COL_QUAD_KEY).ordinal();
		m_mbrColIdx = m_outSchema.getColumn(COL_MBR).ordinal();
		m_trans = CoordinateTransform.getTransformToWgs84(gcInfo.srid());
		
		m_bindOutlier = bindOutlier;
		m_bindOnlyToOwner = bindOnlyToOwner;
		m_binder = new QuadKeyBinder(m_qkeys, m_bindOutlier, m_bindOnlyToOwner);
		
		m_output = input.fstream().flatMap(rec -> attach(rec));
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_input.close();
	}
	
	@Override
	public RecordSchema getRecordSchema() {
		return m_outSchema;
	}
	
	@Override
	public Record nextCopy() {
		return m_output.next().getOrNull();
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
	
	public static RecordSchema calcOutputRecordSchema(RecordSchema inputSchema) {
		return inputSchema.toBuilder() 
							.addOrReplaceColumn(COL_QUAD_KEY, DataType.STRING)
							.addOrReplaceColumn(COL_MBR, DataType.ENVELOPE)
							.build();
	}
	
	@Override
	public String toString() {
		String qkeysStr = FStream.from(m_qkeys).join(',');
		if ( qkeysStr.length() > 40 ) {
			qkeysStr = qkeysStr.substring(0, 40) + "...";
		}
		return String.format("%s: geom=%s, quadkeys=%s", getClass().getSimpleName(),
							m_gcInfo, qkeysStr);
	}
	
	private FStream<Record> attach(Record input) {
		Geometry geom = input.getGeometry(m_geomIdx);
		if ( geom != null && !geom.isEmpty() ) {
			Envelope mbr = geom.getEnvelopeInternal();
			if ( m_validRange == null || m_validRange.intersects(mbr) ) {
				Envelope mbr84 = toWgs84(mbr);
				List<QuadKeyBinding> bindings = m_binder.bindQuadKeys(mbr84);
				if ( bindings.size() > 0 ) {
					return FStream.from(bindings)
									.map(binding -> toOutputRecord(input, binding.quadkey(),
																	binding.mbr4326()));
				}
			}
		}
		if ( m_bindOutlier ) {
			return FStream.of(toOutputRecord(input, OUTLIER_QUADKEY, EMPTY_BOUNDS));
		}
		else {
			return FStream.empty();
		}
	}
	
	private Record toOutputRecord(Record input, String quadKey, Envelope mbr84) {
		Record output = DefaultRecord.of(m_outSchema);
		output.set(input);
		output.set(m_quadKeyColIdx, quadKey);
		output.set(m_mbrColIdx, mbr84);
		
		return output;
	}
	
	private Envelope toWgs84(Envelope envl) {
		return (m_trans != null) ? m_trans.transform(envl) : envl;
	}
}
