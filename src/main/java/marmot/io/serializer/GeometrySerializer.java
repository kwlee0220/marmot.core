package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Geometry;

import marmot.type.DataType;
import marmot.type.DataTypes;
import marmot.type.GeometryDataType;
import marmot.type.TypeCode;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeometrySerializer extends AbstractGeometrySerializer<Geometry> {
	@Override
	public DataType getDataType() {
		return DataType.GEOMETRY;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void serialize(Geometry geom, DataOutput out) {
		// geom이 null인 경우는 typcode에 0만 저장하고,
		// geom이 empty인 경우는 typecode에 음수값만 저장하고,
		// 나머지 경우는 해당 typecode와 geom을 serialize하이 저장한다.
		if ( geom != null ) {
			int tc = GeometryDataType.fromGeometry(geom).getTypeCode().get();
			if ( !geom.isEmpty() ) {
				MarmotSerializers.writeByte((byte)tc, out);
				((AbstractGeometrySerializer)MarmotSerializers.getSerializer(tc)).serialize(geom, out);
			}
			else {
				MarmotSerializers.writeByte((byte)-tc, out);
			}
		}
		else {
			MarmotSerializers.writeByte((byte)0, out);
		}
	}
	
	public Geometry deserialize(DataInput in) {
		int tc = MarmotSerializers.readByte(in);
		if ( tc == 0 ) {
			return null;
		}

		if ( tc > 0 ) {
			if ( !TypeCode.isValid(tc) ) {
				throw new SerializationException("(GeometrySerializer) invalid typecode: " + tc);
			}
			
			DataTypeSerializer<?> ser = MarmotSerializers.getSerializer(tc);
			return (Geometry)ser.deserialize(in);
		}
		else {
			tc = -tc;
			if ( !TypeCode.isValid(tc) ) {
				throw new SerializationException("(GeometrySerializer) invalid typecode: " + tc);
			}
			
			GeometryDataType gtype = (GeometryDataType)DataTypes.fromTypeCode(tc);
			return gtype.newInstance();
		}
	}
}
