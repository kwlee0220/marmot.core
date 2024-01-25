package marmot.optor.support;

import java.util.Map;

import org.mvel2.integration.VariableResolver;
import org.mvel2.integration.impl.BaseVariableResolverFactory;
import org.mvel2.integration.impl.SimpleValueResolver;

import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import marmot.support.DataUtils;
import marmot.support.DefaultRecord;
import utils.CIString;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TransformVariableResolverFactory extends BaseVariableResolverFactory {
	private static final long serialVersionUID = 935743699655716538L;
	
	private final Map<String,Object> m_arguments;
	private final Map<String,VariableResolver> m_resolvers = Maps.newHashMap();
	private Record m_input;
	private Map<String,Object> m_outputMap;
	
	public TransformVariableResolverFactory(RecordSchema outputSchema,
											Map<String,Object> arguments) {
		m_arguments = Maps.newHashMap(arguments);
		
		m_outputMap = DefaultRecord.of(outputSchema).toMap();
		m_resolvers.put("output", new SimpleValueResolver(m_outputMap));
		
		setNextFactory(new ArgumentResolverFactory(arguments));
	}
	
	public void bind(Record input, Record output) {
		m_input = input;
		
		m_resolvers.put("input", new SimpleValueResolver(m_input.toMap()));
		m_outputMap.clear();
	}
	
	public void loadOutputRecord(Record output) {
		output.getRecordSchema()
				.getColumns()
				.stream()
				.forEach(col -> {
					Object v = m_outputMap.get(col.name());
					if ( v != null ) {
						v = DataUtils.cast(v, col.type());
					}
					output.set(col.ordinal(), v);
				});
	}
	
	@Override
	public VariableResolver getVariableResolver(String name) {
		VariableResolver resolver = m_resolvers.get(CIString.of(name));
		if ( resolver != null ) {
			return resolver;
		}

		return getNextFactory().getVariableResolver(name);
	}

	@Override
	public VariableResolver createVariable(String name, Object value) {
		return getNextFactory().createVariable(name, value);
	}

	@Override
	public VariableResolver createVariable(String name, Object value, Class<?> type) {
		return getNextFactory().createVariable(name, value, type);
	}

	@Override
	public boolean isTarget(String name) {
		return m_resolvers.containsKey(CIString.of(name));
	}

	@Override
	public boolean isResolveable(String name) {
		return name.charAt(0) == '$' || isTarget(name)
				|| m_arguments.containsKey(CIString.of(name));
	}
}
