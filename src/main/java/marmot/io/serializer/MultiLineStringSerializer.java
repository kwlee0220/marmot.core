package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;

import marmot.geo.GeoClientUtils;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiLineStringSerializer extends AbstractGeometrySerializer<MultiLineString> {
	private final static LineStringSerializer LINE = new LineStringSerializer();

	@Override
	public DataType getDataType() {
		return DataType.MULTI_LINESTRING;
	}
	
	// nlines (vint)
	//		nlines > 0: 일반적인 LineString
	//		nlines == 0 : empty LineString
	//		nlines == -1: null
	// {points}
	@Override
	public void serialize(MultiLineString mline, DataOutput out) {
		if ( mline != null ) {
			if ( !mline.isEmpty() ) {
				int nlines = mline.getNumGeometries();
				MarmotSerializers.writeVInt(nlines, out);
				for ( int i =0; i < nlines; ++i ) {
					LINE.serialize((LineString)mline.getGeometryN(i), out);
				}
			}
			else {
				MarmotSerializers.writeVInt(0, out);
			}
		}
		else {
			MarmotSerializers.writeVInt(-1, out);
		}
	}

	@Override
	public MultiLineString deserialize(DataInput in) {
		int nlines = MarmotSerializers.readVInt(in);
		if ( nlines > 0 ) {
			LineString[] lines = new LineString[nlines];
			for ( int i =0; i < nlines; ++i ) {
				lines[i] = LINE.deserialize(in);
			}
			
			return GeoClientUtils.GEOM_FACT.createMultiLineString(lines);
		}
		else if ( nlines == 0 ) {
			return GeoClientUtils.EMPTY_MULTILINESTRING;
		}
		else if ( nlines == -1 ) {
			return null;
		}
		else {
			throw new SerializationException("invalid line count: " + nlines);
		}
	}
}
