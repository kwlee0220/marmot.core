package marmot.io.serializer;

import static marmot.io.serializer.MarmotSerializers.POLYGON;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import marmot.geo.GeoClientUtils;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPolygonSerializer extends AbstractGeometrySerializer<MultiPolygon> {
	@Override
	public DataType getDataType() {
		return DataType.MULTI_POLYGON;
	}

	// npolys (vint)
	//		npolys > 0: 일반적인 MultiPolygon
	//		npolys == 0 : empty MultiPolygon
	//		npolys == -1: null
	// {ploygons}
	@Override
	public void serialize(MultiPolygon mpoly, DataOutput out) {
		if ( mpoly != null ) {
			if ( !mpoly.isEmpty() ) {
				int ngeoms = mpoly.getNumGeometries();
				MarmotSerializers.writeVInt(ngeoms, out);
				for ( int i =0; i < ngeoms; ++i ) {
					POLYGON.serialize((Polygon)mpoly.getGeometryN(i), out);
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
	public MultiPolygon deserialize(DataInput in) {
		int npolys = MarmotSerializers.readVInt(in);
		if ( npolys > 0 ) {
			Polygon[] polys = new Polygon[npolys];
			for ( int i =0; i < npolys; ++i ) {
				polys[i] = POLYGON.deserialize(in);
			}
			
			return GeoClientUtils.GEOM_FACT.createMultiPolygon(polys);
		}
		else if ( npolys == 0 ) {
			return GeoClientUtils.EMPTY_MULTIPOLYGON;
		}
		else if ( npolys == -1 ) {
			return null;
		}
		else {
			throw new SerializationException("invalid polygon count: " + npolys);
		}
	}
}
