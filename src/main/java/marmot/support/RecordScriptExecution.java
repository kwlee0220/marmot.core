package marmot.support;

import java.util.Map;

import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.MapVariableResolverFactory;

import com.google.common.collect.Maps;

import marmot.RecordScript;
import marmot.RecordScript.ImportInfo;
import utils.Utilities;
import utils.func.FOption;
import utils.script.MVELScript;
import utils.script.MVELScriptExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordScriptExecution {
	private final MVELScriptExecution m_script;
	private final FOption<MVELScriptExecution> m_initializer;
	private final Map<String,Object> m_arguments;
	
	private static final Class<?>[] FUNC_CLASSES = new Class<?>[] {
		Functions.class, DateTimeFunctions.class, DateFunctions.class, TimeFunctions.class,
		IntervalFunctions.class, MVELFunctions.class, JsonParser.class,
		GeoFunctions.class, TrajectoryFunctions.class,
	};
	
	public static RecordScriptExecution of(RecordScript script) {
		return new RecordScriptExecution(script);
	}

	private RecordScriptExecution(RecordScript rscript) {
		Utilities.checkNotNullArgument(rscript, "RecordScript is null");
		
		MVELScript script = MVELScript.of(rscript.getScript());
		FOption<MVELScript> initializer = rscript.getInitializer()
													.map(MVELScript::of);
		for ( ImportInfo info: rscript.getImportedClassInfoAll() ) {
			script.importClass(info.getImportClass(), info.getImportName());
			initializer.ifPresent(init -> init.importClass(info.getImportClass(), info.getImportName()));
		}

		m_script = MVELScriptExecution.of(script);
		m_initializer = initializer.map(MVELScriptExecution::of);
		
		m_arguments = Maps.newHashMap(rscript.getArgumentAll());
	}
	
	public Map<String,Object> getArgumentAll() {
		return m_arguments;
	}
	
	public RecordScriptExecution addArgument(String name, Object value) {
		m_arguments.put(name, value);
		return this;
	}
	
	public RecordScriptExecution addArgumentAll(Map<String,Object> args) {
		args.forEach((k,v) -> addArgument(k, v));
		return this;
	}
	
	public void initialize(VariableResolverFactory resolverFact) {
		for ( Class<?> funcCls: FUNC_CLASSES ) {
			m_script.importFunctionAll(funcCls);
			m_initializer.ifPresent(init -> init.importFunctionAll(funcCls));
		}
		
		m_initializer.ifPresent(init -> init.run(resolverFact));
	}
	
	public void initialize(Map<String,Object> variables) {
		for ( Class<?> funcCls: FUNC_CLASSES ) {
			m_script.importFunctionAll(funcCls);
			m_initializer.ifPresent(init -> init.importFunctionAll(funcCls));
		}
		
		m_initializer.ifPresent(init -> init.run(new MapVariableResolverFactory(variables)));
	}
	
	public Object execute(VariableResolverFactory resolverFact) {
		return m_script.run(resolverFact);
	}
	
	public Object execute(Map<String,Object> variables) {
		return m_script.run(new MapVariableResolverFactory(variables));
	}
	
	@Override
	public String toString() {
		return m_script.toString();
	}
}
