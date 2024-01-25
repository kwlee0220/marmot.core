package marmot.io;

import org.locationtech.jts.geom.Geometry;

import marmot.Record;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface StoreOutputCollector {
	public void collect(Record record, Geometry geom);
}
