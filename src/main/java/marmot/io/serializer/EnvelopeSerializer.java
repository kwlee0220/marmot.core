package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class EnvelopeSerializer implements DataTypeSerializer<Envelope> {
	@Override
	public DataType getDataType() {
		return DataType.ENVELOPE;
	}
	
	@Override
	public void serialize(Envelope envl, DataOutput out) {
		if ( envl == null ) {
			MarmotSerializers.writeDouble(Double.NaN, out);
		}
		else if ( envl.isNull() ) {
			MarmotSerializers.writeDouble(0, out);
			MarmotSerializers.writeDouble(Double.NaN, out);
		}
		else {
			serializeCoordinate(new Coordinate(envl.getMinX(), envl.getMinY()), out);
			serializeCoordinate(new Coordinate(envl.getMaxX(), envl.getMaxY()), out);
		}
	}

	@Override
	public Envelope deserialize(DataInput in) {
		double minX = MarmotSerializers.readDouble(in);
		if ( Double.isNaN(minX) ) {
			return null;
		}
		double minY = MarmotSerializers.readDouble(in);
		if ( Double.isNaN(minY) ) {
			return new Envelope();
		}
		
		double maxX = MarmotSerializers.readDouble(in);
		double maxY = MarmotSerializers.readDouble(in);
		
		return new Envelope(new Coordinate(minX,minY), new Coordinate(maxX,maxY));
	}
	
	private void serializeCoordinate(Coordinate coord, DataOutput out) {
		MarmotSerializers.writeDouble(coord.x, out);
		MarmotSerializers.writeDouble(coord.y, out);
	}
}
