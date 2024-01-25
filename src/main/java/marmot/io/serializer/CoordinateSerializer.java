package marmot.io.serializer;

import static marmot.io.serializer.MarmotSerializers.readDouble;
import static marmot.io.serializer.MarmotSerializers.writeDouble;

import java.io.DataInput;
import java.io.DataOutput;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Coordinate;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CoordinateSerializer implements MarmotSerializer<Coordinate> {
	@Override
	public void serialize(@Nullable Coordinate coord, DataOutput out) {
		if ( coord == null ) {
			MarmotSerializers.writeDouble(Double.NaN, out);
		}
		else {
			serializeAsIs(coord, out);
		}
	}
	
	public void serializeAsIs(Coordinate coord, DataOutput out) {
		writeDouble(coord.x, out);
		writeDouble(coord.y, out);
	}

	@Override
	public Coordinate deserialize(DataInput in) {
		double x = MarmotSerializers.readDouble(in);
		if ( Double.isNaN(x) ) {
			return null;
		}
		else {
			return new Coordinate(x, readDouble(in));
		}
	}
}
