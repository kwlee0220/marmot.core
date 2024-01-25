package marmot.dataset;

import com.google.common.base.Preconditions;

import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Catalogs {
	public static final char ID_DELIM = '/';
	
	public static String normalize(String dsId) {
		Utilities.checkNotNullArgument(dsId, "dataset is is null");
		
		dsId = dsId.trim();
		if ( dsId.length() == 0 || dsId.charAt(0) != ID_DELIM ) {
			dsId = ID_DELIM + dsId;
		}
		if ( dsId.length() > 1 && dsId.charAt(dsId.length()-1) == ID_DELIM ) {
			dsId = dsId.substring(0, dsId.length()-1);
		}
		
		return dsId;
	}
	
	public static String toDataSetId(String folder, String name) {
		Utilities.checkNotNullArgument(folder, "dataset folder is is null");
		Utilities.checkNotNullArgument(name, "dataset name is is null");
		Preconditions.checkArgument(name.indexOf(ID_DELIM) < 0, "invalid name: " + name);
		
		return normalize(folder) + ID_DELIM + name;
	}
	
	public static String getFolder(String dsId) {
		dsId = normalize(dsId);
		
		int idx = dsId.lastIndexOf(ID_DELIM);
		return (idx > 0) ? dsId.substring(0, idx) : "/";
	}
	
	public static String getName(String dsId) {
		dsId = normalize(dsId);
		
		int idx = dsId.lastIndexOf(ID_DELIM);
		return dsId.substring(idx+1);	
	}
}
