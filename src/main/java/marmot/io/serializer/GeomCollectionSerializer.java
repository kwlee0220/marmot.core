package marmot.io.serializer;

import static marmot.io.serializer.MarmotSerializers.GEOMETRY;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;

import marmot.geo.GeoClientUtils;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeomCollectionSerializer extends AbstractGeometrySerializer<GeometryCollection> {
	@Override
	public DataType getDataType() {
		return DataType.GEOM_COLLECTION;
	}

	@Override
	public void serialize(GeometryCollection coll, DataOutput out) {
		if ( coll != null ) {
			if ( !coll.isEmpty() ) {
				int ngeoms = coll.getNumGeometries();
				MarmotSerializers.writeVInt(ngeoms, out);
				for ( int i =0; i < ngeoms; ++i ) {
					GEOMETRY.serialize(coll.getGeometryN(i), out);
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
	public GeometryCollection deserialize(DataInput in) {
		int ngeoms = MarmotSerializers.readVInt(in);
		if ( ngeoms > 0 ) {
			Geometry[] geoms = new Geometry[ngeoms];
			for ( int i =0; i < ngeoms; ++i ) {
				geoms[i] = GEOMETRY.deserialize(in);
			}
			
			return GeoClientUtils.GEOM_FACT.createGeometryCollection(geoms);
		}
		else if ( ngeoms == 0 ) {
			return GeoClientUtils.EMPTY_GEOM_COLLECTION;
		}
		else if ( ngeoms == -1 ) {
			return null;
		}
		else {
			throw new SerializationException("invalid geometry count: " + ngeoms);
		}
	}
}
