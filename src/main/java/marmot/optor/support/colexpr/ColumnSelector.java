package marmot.optor.support.colexpr;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnSelector implements Serializable {
	private static final long serialVersionUID = -883106526001123752L;
	
	private final String m_colExpr;
	private final Set<SelectedColumnInfo> m_columnInfos;
	private final RecordSchema m_schema;
	
	ColumnSelector(String columnExpr, Set<SelectedColumnInfo> colInfos, RecordSchema schema) {
		m_colExpr = columnExpr;
		m_columnInfos = colInfos;
		m_schema = schema;
	}
	
	public String getColumnExpression() {
		return m_colExpr;
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public Set<SelectedColumnInfo> getColumnSelectionInfoAll() {
		return Collections.unmodifiableSet(m_columnInfos);
	}
	
	public FOption<Column> findColumn(String namespace, String colName) {
		return FStream.from(m_columnInfos)
					.findFirst(c -> c.m_namespace.equals(namespace) && c.m_column.name().equals(colName))
					.map(c -> new Column(c.m_alias, c.m_column.type()));
	}
	
	public Record select(Map<String,Record> source) {
		Record output = DefaultRecord.of(m_schema);
		select(source, output);
		return output;
	}
	
	public void select(Map<String,Record> source, Record output) {
		int i = 0;
		for ( SelectedColumnInfo info: m_columnInfos ) {
			Record input = source.getOrDefault(info.m_namespace, null);
			Object value = (input != null)
						? input.get(info.m_column.ordinal())
						: null;
			output.set(i++, value);
		}
	}
	
	public void select(Record input, Record output) {
		int i = 0;
		for ( SelectedColumnInfo info: m_columnInfos ) {
			output.set(i++, input.get(info.m_column.ordinal()));
		}
	}
	
	public Record select(Record input) {
		Record output = DefaultRecord.of(m_schema);
		select(input, output);
		
		return output;
	}
	
	public Object[] select(Object[] input) {
		return FStream.from(m_columnInfos)
						.map(info -> input[info.m_column.ordinal()])
						.toList().toArray();
	}
	
	public RecordSet select(Record outer, RecordSet inners) {
		return new NestedLoopRecordSet(outer, inners, this);
	}
	
	@Override
	public String toString() {
		return m_colExpr;
	}
	
	private static class NestedLoopRecordSet extends AbstractRecordSet {
		private final RecordSet m_inners;
		private final Record m_innerRecord;
		private final ColumnSelector m_selector;
		private final Map<String,Record> m_binding = Maps.newHashMap();
		
		private NestedLoopRecordSet(Record outer, RecordSet inners, ColumnSelector selector) {
			m_inners = inners;
			m_innerRecord = DefaultRecord.of(inners.getRecordSchema());
			m_selector = selector;
			m_binding.put("left", outer);
		}
		
		@Override protected void closeInGuard() { }

		@Override
		public RecordSchema getRecordSchema() {
			return m_selector.getRecordSchema();
		}
		
		@Override
		public boolean next(Record output) {
			if ( m_inners.next(m_innerRecord) ) {
				m_binding.put("right", m_innerRecord);
				m_selector.select(m_binding, output);
				
				return true;
			}
			else {
				return false;
			}
		}
	}
}
