package marmot.support;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import com.google.common.collect.Maps;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HadoopUtils {
	private HadoopUtils() {
		throw new AssertionError("Should not be called: class=" + getClass().getName());
	}
	
	public static String replaceVariables(Configuration conf, String prefix, String str) {
		Map<String,String> mappings = StreamSupport.stream(conf.spliterator(), false)
													.filter(entry -> entry.getKey().startsWith(prefix))
													.collect(Collectors.toMap(entry->entry.getKey(),
																				entry->entry.getValue()));
		return replaceVariables(str, mappings);
	}
	
	public static String replaceVariables(String str, Map<String,String> mappings) {
		StrSubstitutor substitutor = new StrSubstitutor(mappings);
		return substitutor.replace(str);
	}
	
	private static final String COMPRESS_CODEC_ZIP = "org.apache.hadoop.io.compress.DefaultCodec";
	private static final String COMPRESS_CODEC_GZIP = "org.apache.hadoop.io.compress.GzipCodec";
	private static final String COMPRESS_CODEC_SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";
	private static final String COMPRESS_CODEC_LZ4 = "org.apache.hadoop.io.compress.Lz4Codec";
	private static final String COMPRESS_CODEC_LZO = "com.hadoop.compression.lzo.LzoCodec";
	private static final Map<String,String> CODECS = Maps.newHashMap();
	static {
		CODECS.put("snappy", COMPRESS_CODEC_SNAPPY);
		CODECS.put("gzip", COMPRESS_CODEC_GZIP);
		CODECS.put("gz", COMPRESS_CODEC_GZIP);
		CODECS.put("zip", COMPRESS_CODEC_ZIP);
		CODECS.put("lz4", COMPRESS_CODEC_LZ4);
		CODECS.put("lzo", COMPRESS_CODEC_LZO);
	}
	
	public static boolean isValidCompressionCodecName(String codecName) {
		return CODECS.containsKey(codecName);
	}
	
	public static CompressionCodec getCompressionCodecByClassName(Configuration conf, String clsName) {
		Utilities.checkNotNullArgument(conf, "Configuration");
		Utilities.checkNotNullArgument(clsName, "compression codec class name");
		
		return new CompressionCodecFactory(conf).getCodecByClassName(clsName);
	}
	
	public static String getCompressionCodecClassByName(String codecName) {
		Utilities.checkNotNullArgument(codecName, "compression codec name");
		
		String clsName = CODECS.get(codecName.toLowerCase());
		if ( clsName == null ) {
			throw new IllegalArgumentException("invalid codec name: " + codecName);
		}
		
		return clsName;
	}
	
	public static CompressionCodec getCompressionCodecByName(Configuration conf, String codecName) {
		Utilities.checkNotNullArgument(codecName, "compression codec name");
		
		String clsName = CODECS.get(codecName.toLowerCase());
		if ( clsName == null ) {
			throw new IllegalArgumentException("invalid codec name: " + codecName);
		}
		
		return getCompressionCodecByClassName(conf, getCompressionCodecClassByName(codecName));
	}
	
	public static CompressionOutputStream toCompressionStream(OutputStream out,
																CompressionCodec codec)
		throws IOException {
		return codec.createOutputStream(out);
	}
}
