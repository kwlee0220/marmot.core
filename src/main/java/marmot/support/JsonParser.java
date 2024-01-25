package marmot.support;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import utils.script.MVELFunction;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JsonParser {
//	private static final Type TYPE = new TypeToken<Map<String,Object>>(){}.getType();
//	
//	public static Map<String,Object> parse(File jsonFile) throws IOException {
//		try ( FileReader reader = new FileReader(jsonFile) ) {
//			return new Gson().fromJson(reader, TYPE);
//		}
//	}
//	
//	@MVELFunction(name="ST_ParseJSON")
//	public static Map<String,Object> parse(String jsonStr) {
//		return new Gson().fromJson(jsonStr, TYPE);
//	}
	
	private static final ObjectMapper OM = new ObjectMapper();
	private static final TypeReference<Map<String,Object>> VALUE_TYPE
													= new TypeReference<Map<String,Object>>(){};
	static {
		OM.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
	}

	@MVELFunction(name="ST_ParseJSON")
	public static Map<String,Object> parse(String jsonStr)
		throws JsonParseException, JsonMappingException, IOException {
		jsonStr = jsonStr.substring(jsonStr.indexOf('{'));
		return OM.readValue(jsonStr, VALUE_TYPE);
	}

	public static Map<String,Object> parse(File jsonFile) throws JsonParseException, JsonMappingException, IOException {
		return OM.readValue(jsonFile, VALUE_TYPE);
	}
	
	public static void write(Writer writer, Object json) throws JsonGenerationException, JsonMappingException, IOException {
		OM.writeValue(writer, json);
	}
	
	public static String toJsonString(Object json) throws JsonGenerationException, JsonMappingException, IOException {
		StringWriter writer = new StringWriter();
		write(writer, json);
		return writer.toString();
	}
}
