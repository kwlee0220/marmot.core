package marmot.io.serializer;

import static marmot.io.serializer.MarmotSerializers.readDouble;
import static marmot.io.serializer.MarmotSerializers.readVInt;
import static marmot.io.serializer.MarmotSerializers.writeDouble;
import static marmot.io.serializer.MarmotSerializers.writeVInt;

import java.io.DataInput;
import java.io.DataOutput;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractGeometrySerializer<T extends Geometry> implements DataTypeSerializer<T> {
	static Coordinate readCoordinate(DataInput in) {
		return new Coordinate(readDouble(in), readDouble(in));
	}
	
	static void writeCoordinate(Coordinate coord, DataOutput out) {
		writeDouble(coord.x, out);
		writeDouble(coord.y, out);
	}
	
	protected Coordinate[] readCoordinates(DataInput in) {
		int ncoords = readVInt(in);
		
		Coordinate[] coords = new Coordinate[ncoords];
		for ( int i =0; i < ncoords; ++i ) {
			coords[i] = readCoordinate(in);
		}
		
		return coords;
	}
	
	protected Coordinate[] readCoordinates(int count, DataInput in) {
		Coordinate[] coords = new Coordinate[count];
		for ( int i =0; i < count; ++i ) {
			coords[i] = readCoordinate(in);
		}
		
		return coords;
	}
	
	protected void writeCoordinates(Coordinate[] coords, DataOutput out) {
		writeVInt(coords.length, out);
		for ( Coordinate coord: coords ) {
			writeCoordinate(coord, out);
		}
	}
}
