package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;
import marmot.type.MapTile;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapTileSerializer implements DataTypeSerializer<MapTile> {
	@Override
	public DataType getDataType() {
		return DataType.TILE;
	}
	
	@Override
	public void serialize(MapTile tile, DataOutput out) {
		MarmotSerializers.writeByte((byte)tile.getZoom(), out);
		MarmotSerializers.writeInt(tile.getX(), out);
		MarmotSerializers.writeInt(tile.getY(), out);
	}

	@Override
	public MapTile deserialize(DataInput in) {
		byte zoom = MarmotSerializers.readByte(in);
		int x = MarmotSerializers.readInt(in);
		int y = MarmotSerializers.readInt(in);
		
		return new MapTile(zoom, x, y);
	}
}
