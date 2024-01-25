package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import marmot.geo.GeoClientUtils;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PolygonSerializer extends AbstractGeometrySerializer<Polygon> {
	private final static LineStringSerializer RING = MarmotSerializers.LINE;

	@Override
	public DataType getDataType() {
		return DataType.MULTI_POLYGON;
	}

	@Override
	public void serialize(Polygon poly, DataOutput out) {
		if ( poly != null ) {
			if ( !poly.isEmpty() ) {
				RING.serialize(poly.getExteriorRing(), out);
				
				int nholes = poly.getNumInteriorRing();
				MarmotSerializers.writeVInt(nholes, out);
				for ( int i =0; i < nholes; ++i ) {
					RING.serialize(poly.getInteriorRingN(i), out);
				}
			}
			else {
				RING.serialize(GeoClientUtils.EMPTY_LINEARRING, out);
			}
		}
		else {
			RING.serialize((LineString)null, out);
		}
	}

	@Override
	public Polygon deserialize(DataInput in) {
		LinearRing shell = RING.deserializeLinearRing(in);
		if ( shell == null ) {
			return null;
		}
		else if ( !shell.isEmpty() ) {
			int nholes = MarmotSerializers.readVInt(in);
			LinearRing[] holes = new LinearRing[nholes];
			for ( int i =0; i < nholes; ++i ) {
				holes[i] = RING.deserializeLinearRing(in);
			}
			
			return GeoClientUtils.GEOM_FACT.createPolygon(shell, holes);
		}
		else {
			return GeoClientUtils.EMPTY_POLYGON;
		}
	}
}
