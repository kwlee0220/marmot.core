package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import marmot.geo.GeoClientUtils;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PointSerializer extends AbstractGeometrySerializer<Point> {
	private static final Coordinate NULL_POINT = new Coordinate(Double.NaN, Double.NaN);
	private static final Coordinate EMPTY_POINT = new Coordinate(Double.NaN, 0);
	
	@Override
	public DataType getDataType() {
		return DataType.POINT;
	}

	@Override
	public void serialize(Point pt, DataOutput out) {
		if ( pt != null ) {
			if ( !pt.isEmpty() ) {
				writeCoordinate(pt.getCoordinate(), out);
			}
			else {
				writeCoordinate(EMPTY_POINT, out);
			}
		}
		else {
			writeCoordinate(NULL_POINT, out);
		}
	}

	@Override
	public Point deserialize(DataInput in) {
		Coordinate coord = readCoordinate(in);
		if ( !Double.isNaN(coord.x) ) {
			return GeoClientUtils.toPoint(coord);
		}
		else if ( Double.isNaN(coord.y) ) {
			return null;
		}
		else {
			return GeoClientUtils.EMPTY_POINT;
		}
	}
}
