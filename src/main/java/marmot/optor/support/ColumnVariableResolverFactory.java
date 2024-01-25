package marmot.optor.support;

import java.util.Map;

import org.mvel2.integration.VariableResolver;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.BaseVariableResolverFactory;

import com.google.common.collect.Maps;

import marmot.Record;
import marmot.RecordSchema;
import utils.CIStringMap;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnVariableResolverFactory extends BaseVariableResolverFactory {
	private static final long serialVersionUID = 1L;
	
	private final RecordSchema m_schema;
	private final CIStringMap<ColumnResolver> m_resolvers = CIStringMap.newHashMap();
	private Record m_record;
	private boolean m_readonly = false;
	
	public ColumnVariableResolverFactory(RecordSchema schema, Map<String,Object> arguments) {
		m_schema = schema;
		
		VariableResolverFactory argsFact = new ArgumentResolverFactory(arguments);
		argsFact.setNextFactory(new TempVariableResolverFactory());
		setNextFactory(argsFact);
	}
	
	public ColumnVariableResolverFactory(RecordSchema schema) {
		this(schema, Maps.newHashMap());
	}
	
	public ColumnVariableResolverFactory readOnly(boolean flag) {
		m_readonly = flag;
		return this;
	}
	
	public void bind(Record record) {
		m_record = record;
		m_resolvers.clear();
	}
	
	public VariableResolver getVariableResolver(String name) {
		return findColumnResolver(name)
					.getOrElse(() -> getNextFactory().getVariableResolver(name));
	}

	@Override
	public VariableResolver createVariable(String name, Object value) {
		FOption<VariableResolver> resolver = findColumnResolver(name);
		if ( m_readonly && resolver.isPresent() ) {
			throw new IllegalStateException("cannot update column: name=" + name);
		}
		
		return resolver.ifPresent(r -> r.setValue(value))
						.getOrElse(() -> getNextFactory().createVariable(name, value));
	}
	

	@Override
	public VariableResolver createVariable(String name, Object value, Class<?> type) {
		FOption<VariableResolver> resolver = findColumnResolver(name);
		if ( m_readonly && resolver.isPresent() ) {
			throw new IllegalStateException("cannot update column: name=" + name);
		}
		
		return resolver.ifPresent(r -> {
							r.setStaticType(type);
							r.setValue(value);
						})
						.getOrElse(() -> getNextFactory().createVariable(name, value, type));
	}

	@Override
	public boolean isTarget(String name) {
		return m_schema.existsColumn(name);
	}

	@Override
	public boolean isResolveable(String name) {
		VariableResolverFactory fact = getNextFactory();
		return isTarget(name) || (fact != null && fact.isResolveable(name));
	}
	
	private FOption<VariableResolver> findColumnResolver(String name) {
		ColumnResolver resolver = m_resolvers.get(name);
		if ( resolver != null ) {
			return FOption.of(resolver);
		}
		
		return m_schema.findColumn(name)
						.map(col -> {
							ColumnResolver res = new ColumnResolver(m_record, col);
							m_resolvers.put(name, res);
							
							return res;
						});
	}
}
