package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.MultiPoint;

import marmot.geo.GeoClientUtils;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MultiPointSerializer extends AbstractGeometrySerializer<MultiPoint> {
	@Override
	public DataType getDataType() {
		return DataType.MULTI_POINT;
	}

	// npoints (vint)
	//		npoints > 0: 일반적인 LineString
	//		npoints == 0 : empty LineString
	//		npoints == -1: null
	// {points}
	@Override
	public void serialize(MultiPoint mpt, DataOutput out) {
		if ( mpt != null ) {
			if ( !mpt.isEmpty() ) {
				writeCoordinates(mpt.getCoordinates(), out);
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
	public MultiPoint deserialize(DataInput in) {
		int npoints = MarmotSerializers.readVInt(in);
		if ( npoints > 0 ) {
			return GeoClientUtils.GEOM_FACT.createMultiPoint(readCoordinates(npoints, in));
		}
		else if ( npoints == 0 ) {
			return GeoClientUtils.EMPTY_MULTIPOINT;
		}
		else if ( npoints == -1 ) {
			return null;
		}
		else {
			throw new SerializationException("invalid point count: " + npoints);
		}
	}
}
