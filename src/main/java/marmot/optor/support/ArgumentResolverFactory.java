package marmot.optor.support;

import java.util.Map;

import org.mvel2.integration.VariableResolver;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.BaseVariableResolverFactory;
import org.mvel2.integration.impl.SimpleValueResolver;

import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ArgumentResolverFactory extends BaseVariableResolverFactory {
	private static final long serialVersionUID = 4819032803085887012L;
	private final Map<String,VariableResolver> m_resolvers;
	
	ArgumentResolverFactory(Map<String,Object> arguments) {
		m_resolvers = KVFStream.from(arguments)
								.mapValue(v -> (VariableResolver)new SimpleValueResolver(v))
								.toMap();
	}
	
	public VariableResolver getVariableResolver(String name) {
		VariableResolver resolver = m_resolvers.get(name);
		if ( resolver != null ) {
			return resolver;
		}
		else {
			return getNextFactory().getVariableResolver(name);
		}
	}

	@Override
	public VariableResolver createVariable(String name, Object value) {
		return getNextFactory().createVariable(name, value);
	}

	@Override
	public VariableResolver createVariable(String name, Object value, Class<?> type) {
		return getNextFactory().createVariable(name, value);
	}

	@Override
	public boolean isTarget(String name) {
		return m_resolvers.containsKey(name);
	}

	@Override
	public boolean isResolveable(String name) {
		VariableResolverFactory fact = getNextFactory();
		return m_resolvers.containsKey(name)
				|| (fact != null && fact.isResolveable(name));
	}
}
