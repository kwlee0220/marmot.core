package marmot.optor.support;

import org.mvel2.integration.VariableResolver;
import org.mvel2.integration.impl.MapVariableResolverFactory;

/** 
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class TempVariableResolverFactory extends MapVariableResolverFactory {
	private static final long serialVersionUID = 1213457724968423690L;

	@Override
	public VariableResolver createVariable(String name, Object value) {
		if ( isTarget(name) ) {
			return super.createVariable(name, value);
		}
		else {
			throw new RuntimeException("cannot create variable: name=" + name);
		}
	}
	

	@Override
	public VariableResolver createVariable(String name, Object value, Class<?> type) {
		if ( isTarget(name) ) {
			return super.createVariable(name, value, type);
		}
		else {
			throw new RuntimeException("cannot create variable: name=" + name);
		}
	}

	@Override
	public boolean isTarget(String name) {
		return isTemporary(name);
	}

	@Override
	public boolean isResolveable(String name) {
	    return (isTemporary(name))
	            || (nextFactory != null && nextFactory.isResolveable(name));
	}

	private boolean isTemporary(String name) {
		char prefix = name.charAt(0);
		return prefix == '$' || prefix == '_';
	}
}
