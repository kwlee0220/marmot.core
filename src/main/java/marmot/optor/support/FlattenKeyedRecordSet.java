package marmot.optor.support;

import org.slf4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.ProgressReportable;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class FlattenKeyedRecordSet extends AbstractRecordSet implements ProgressReportable {
	private final KeyedRecordSet m_keyed;
	private final RecordSchema m_schema;
	private final Object[] m_keys;
	private final Record m_value;
	
	FlattenKeyedRecordSet(KeyedRecordSet keyed) {
		m_keyed = keyed;
		m_schema = RecordSchema.concat(m_keyed.getKeySchema(), m_keyed.getRecordSchema());
		m_value = DefaultRecord.of(m_keyed.getRecordSchema());
		m_keys = keyed.getKey().getValues();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_keyed.close();
	}
	
	@Override
	public boolean next(Record output) {
		if ( m_keyed.next(m_value) ) {
			output.setAll(0, m_keys);
			output.setAll(m_keys.length, m_value.getAll());
			
			return true;
		}
		else {
			return false;
		}
	}
	
	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		if (m_keyed instanceof ProgressReportable ) {
			((ProgressReportable)m_keyed).reportProgress(logger, elapsed);
		}
	}
}
