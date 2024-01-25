package marmot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import picocli.CommandLine.Option;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Tuple;


/**
 * Marmot 접속을 위한 Configuration 객체 builder 클래스.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConfigurationBuilder {
	private static final Logger s_logger = LoggerFactory.getLogger(ConfigurationBuilder.class);
	
	private static final String[] EMPTY_ARGS = new String[0];
	private static final String HADOOP_CONFIG = "hadoop-conf";

	@Nullable private String m_homeDir = null;
	@Nullable private File m_configDir = null;
	@Nullable private String m_lock = null;
	private String m_runnerMode = "local-mr";
	private boolean m_runAtCluster = false;
	
	public FOption<File> getHomeDir() {
		return FOption.ofNullable(m_homeDir)
						.orElse(() -> FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME")))
						.map(File::new);
	}

	@Option(names={"-home"}, paramLabel="path", description={"Marmot home directory"})
	public ConfigurationBuilder setHomeDir(File dir) {
		Utilities.checkArgument(dir.isDirectory(), "HomeDir is not a directory: " + dir);
		
		return setConfigDir(new File(dir, HADOOP_CONFIG));
	}
	
	public FOption<File> getConfigDir() {
		return FOption.ofNullable(m_configDir);
	}

	@Option(names={"-config"}, paramLabel="path", description={"Marmot config directory"})
	public ConfigurationBuilder setConfigDir(File dir) {
		Utilities.checkArgument(dir.isDirectory(), "ConfigDir is not a directory: " + dir);
		
		m_configDir = dir;
		return this;
	}

	public FOption<File> getTerminationLockFile() {
		return FOption.ofNullable(m_lock).map(File::new);
	}

	@Option(names={"-lock"}, paramLabel="path", description={"MarmotServer termination-lock file"})
	public ConfigurationBuilder setTerminationLockFile(String path) {
		m_lock = path;
		return this;
	}

	@Option(names={"-runner"}, paramLabel="mode",
			description={"Marmot runner-mode ('local', 'local-mr', 'mr'"})
	public ConfigurationBuilder setRunnerMode(String mode) {
		m_runnerMode = mode;
		return this;
	}
	
	public ConfigurationBuilder runAtCluster(boolean flag) {
		m_runAtCluster = flag;
		return this;
	}
	
	public Configuration build() {
		try {
			Driver driver = new Driver();
			ToolRunner.run(driver, EMPTY_ARGS);
			
			return loader(driver.getConf(), driver.m_args)._1;
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}
	
	public Tuple<Configuration,String[]> build(String... args) throws Exception {
		Utilities.checkNotNullArgument(args != null, "empty arguments");
		
		Driver driver = new Driver();
		ToolRunner.run(driver, args);
		
		return loader(driver.getConf(), driver.m_args);
	}
	
	public ConfigurationBuilder forMR() {
		return setRunnerMode("mr");
	}
	
	public ConfigurationBuilder forLocalMR() {
		return setRunnerMode("local-mr");
	}
	
	public ConfigurationBuilder forLocal() {
		return setRunnerMode("local");
	}
	
	public static String[] toApplicationArguments(String... args) throws Exception {
		Driver driver = new Driver();
		ToolRunner.run(driver, EMPTY_ARGS);
		
		return driver.m_args;
	}
	
	private InputStream readMarmotResource(String name) throws FileNotFoundException {
		if ( m_configDir != null ) {
			return new FileInputStream(new File(m_configDir, name));
		}
		else {
			name = String.format("%s/%s", HADOOP_CONFIG, name);
			return Thread.currentThread().getContextClassLoader()
										.getResourceAsStream(name);
		}
	}
	
	private Tuple<Configuration,String[]> loader(Configuration conf, String[] applArgs)
		throws Exception {
		Utilities.checkNotNullArgument(m_runnerMode != null, "runner mode is not specified");
		
		if ( !m_runAtCluster && m_configDir == null ) {
			setHomeDir(getHomeDir().getOrThrow(() -> new IllegalStateException("Marmot config directory is not specified")));
		}
		
		// Marmot 설정 정보 추가
		conf.addResource(readMarmotResource("marmot.xml"));
		
		// Marmot runner 설정 추가
		switch ( m_runnerMode ) {
			case "local":
			case "local-mr":
			case "mr":
				conf.addResource(readMarmotResource(String.format("marmot-%s.xml", m_runnerMode)));
				break;
			default:
				throw new IllegalArgumentException("invalid plan runner: " + m_runnerMode);
		}
		
		return Tuple.of(conf, applArgs);
	}

	private static class Driver extends Configured implements Tool {
		private String[] m_args;
		
		@Override
		public int run(String[] args) throws Exception {
			m_args = args;
			return 0;
		}
	}
	
	public static File getLog4jPropertiesFile() {
		String homeDir = FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME"))
								.getOrElse(() -> System.getProperty("user.dir"));
		return new File(homeDir, "log4j.properties");
	}
	
	public static File configureLog4j() throws IOException {
		File propsFile = getLog4jPropertiesFile();
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		
		Map<String,String> bindings = Maps.newHashMap();
		bindings.put("marmot.home", propsFile.getParentFile().toString());

		String rfFile = props.getProperty("log4j.appender.rfout.File");
		rfFile = StringSubstitutor.replace(rfFile, bindings);
		props.setProperty("log4j.appender.rfout.File", rfFile);
		PropertyConfigurator.configure(props);
		
		return propsFile;
	}
}
