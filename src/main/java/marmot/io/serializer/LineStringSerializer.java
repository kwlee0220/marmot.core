package marmot.io.serializer;

import static marmot.io.serializer.MarmotSerializers.writeVInt;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;

import marmot.geo.GeoClientUtils;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LineStringSerializer extends AbstractGeometrySerializer<LineString> {
	@Override
	public DataType getDataType() {
		return DataType.LINESTRING;
	}

	// npoints (vint)
	//		npoints > 0: 일반적인 LineString
	//		npoints == 0 : empty LineString
	//		npoints == -1: null
	// {points}
	@Override
	public void serialize(LineString line, DataOutput out) {
		if ( line != null ) {
			if ( !line.isEmpty() ) {
				Coordinate[] points = line.getCoordinates();
				writeVInt(points.length, out);
				for ( Coordinate pt: points ) {
					writeCoordinate(pt, out);
				}
			}
			else {
				writeVInt(0, out);
			}
		}
		else {
			writeVInt(-1, out);
		}
	}

	@Override
	public LineString deserialize(DataInput in) {
		int npoints = MarmotSerializers.readVInt(in);
		if ( npoints > 0 ) {
			Coordinate[] points = new Coordinate[npoints];
			for ( int i =0; i < npoints; ++i ) {
				points[i] = readCoordinate(in);
			}
			
			return GeoClientUtils.GEOM_FACT.createLineString(points);
		}
		else if ( npoints == 0 ) {
			return GeoClientUtils.EMPTY_LINESTRING;
		}
		else {
			return null;
		}
	}
	
	public LinearRing deserializeLinearRing(DataInput in) {
		int npoints = MarmotSerializers.readVInt(in);
		if ( npoints > 0 ) {
			return GeoClientUtils.GEOM_FACT.createLinearRing(readCoordinates(npoints, in));
		}
		else if ( npoints == 0 ) {
			return GeoClientUtils.EMPTY_LINEARRING;
		}
		else if ( npoints == -1 ) {
			return null;
		}
		else {
			throw new SerializationException("invalid point count: " + npoints);
		}
	}
}
