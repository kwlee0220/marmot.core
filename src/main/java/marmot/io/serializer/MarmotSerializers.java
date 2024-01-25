package marmot.io.serializer;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.io.WritableUtils;

import marmot.Column;
import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.dataset.GeometryColumnInfo;
import marmot.type.DataType;
import marmot.type.TypeCode;
import utils.Size2d;
import utils.Utilities;
import utils.func.CheckedBiConsumer;
import utils.func.CheckedConsumerX;
import utils.func.CheckedFunction;
import utils.func.CheckedFunctionX;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotSerializers {
	private static final Charset CHARSET = StandardCharsets.UTF_8;
	public static final EnvelopeSerializer ENVELOPE = new EnvelopeSerializer();
	public static final RecordSchemaSerializer RECORD_SCHEMA = new RecordSchemaSerializer();
	public static final CoordinateSerializer COORDINATE = new CoordinateSerializer();
	public static final LineStringSerializer LINE = new LineStringSerializer();
	public static final PolygonSerializer POLYGON = new PolygonSerializer();
	public static final GeometrySerializer GEOMETRY = new GeometrySerializer();
	
	private static DataTypeSerializer<?>[] SERDES = new DataTypeSerializer[] {
		null,	// 0
		new ByteSerializer(),			// BYTE
		new ShortSerializer(),			// SHORT
		new IntSerializer(),			// INT
		new LongSerializer(),			// LONG
		new FloatSerializer(),			// FLOAT
		new DoubleSerializer(),			// DOUBLE
		new BooleanSerializer(),		// BOOLEAN
		new StringSerializer(),			// STRING
		new BinarySerializer(),			// BINARY
		new TypedObjectSerializer(),	// TYPED (10)
		new FloatArraySerializer(),		// ARRAY_FLOAT (11)
		new DoubleArraySerializer(),	// ARRAY_DOUBLE (12)
		null, null, null,			// (12 ~ 15)
		new DateTimeSerializer(),		// DATE_TIME (16)
		new DateSerializer(),			// DATE
		new TimeSerializer(),			// TIME
		new DurationSerializer(),		// DURATION
		new IntervalSerializer(),		// INTERVAL (20)
		null, null, null, null, null,	// (21 ~ 25)
		ENVELOPE,						// ENVELOPE (26)
		new MapTileSerializer(),		// MAP_TILE
		new GridCellSerializer(),		// GRID_CELL (28)
		null, null,						// (29 ~ 30)
		new PointSerializer(),			// POINT (31)
		new MultiPointSerializer(),		// MULTI_POINT
		LINE,							// LINE
		new MultiLineStringSerializer(),// MULTI_LINE
		POLYGON,						// POLYGON
		new MultiPolygonSerializer(),	// MULTI_POLYGON
		new GeomCollectionSerializer(),	// GEOM_COLLECTION
		GEOMETRY,						// GEOMETRY (38)
		null, null,						// (39 ~ 40)
		new TrajectorySerializer(),		// TRAJECTORY (41)
	};
	
	public static DataTypeSerializer<?> getSerializer(int tc) {
		return SERDES[tc];
	}
	
	public static DataTypeSerializer<?> getSerializer(TypeCode tc) {
		return SERDES[tc.get()];
	}
	
	public static DataTypeSerializer<?> getSerializer(DataType type) {
		return SERDES[type.getTypeCode().get()];
	}
	
	public static void deserialize(byte[] bytes, Record output) {
		RecordSchema schema = output.getRecordSchema();

		try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes)) ) {
			for ( Column col: schema.getColumns() ) {
				MarmotSerializer<?> serde = MarmotSerializers.getSerializer(col.type());
				output.set(col.ordinal(), serde.deserialize(dis));
			}
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to deserialize record", e);
		}
	}

	public static byte readByte(DataInput in) {
		try {
			return in.readByte();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeByte(byte v, DataOutput out) {
		try {
			out.writeByte(v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static int readInt(DataInput in) {
		try {
			return in.readInt();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeInt(int v, DataOutput out) {
		try {
			out.writeInt(v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static int readVInt(DataInput in) {
		try {
			return (int)WritableUtils.readVLong(in);
		}
		catch ( Exception e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeVInt(int v, DataOutput out) {
		try {
			WritableUtils.writeVLong(out, v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static long readLong(DataInput in) {
		try {
			return in.readLong();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeLong(long v, DataOutput out) {
		try {
			out.writeLong(v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static long readVLong(DataInput in) {
		try {
			return WritableUtils.readVLong(in);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeVLong(long v, DataOutput out) {
		try {
			WritableUtils.writeVLong(out, v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static short readShort(DataInput in) {
		try {
			return in.readShort();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeShort(short v, DataOutput out) {
		try {
			out.writeShort(v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static float readFloat(DataInput in) {
		try {
			return in.readFloat();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeFloat(float v, DataOutput out) {
		try {
			out.writeFloat(v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static double readDouble(DataInput in) {
		try {
			return in.readDouble();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeDouble(double v, DataOutput out) {
		try {
			out.writeDouble(v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static String readString(DataInput in) {
		byte[] bytes = readBinary(in);
		return new String(bytes, CHARSET);
	}

	public static void writeString(String v, DataOutput out) {
		byte[] bytes = v.getBytes(CHARSET);
		writeBinary(bytes, out);
	}

	public static boolean readBoolean(DataInput in) {
		try {
			return in.readBoolean();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeBoolean(boolean v, DataOutput out) {
		try {
			out.writeBoolean(v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static byte[] readBinary(DataInput in) {
		try {
			int length = (int)WritableUtils.readVLong(in);
			byte[] buffer = new byte[length];
			in.readFully(buffer);
			
			return buffer;
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static void writeBinary(byte[] v, DataOutput out) {
		try {
			WritableUtils.writeVLong(out, v.length);
			out.write(v);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static Size2d readSize2d(DataInput in) {
		double width = readDouble(in);
		double height = readDouble(in);
		return new Size2d(width, height);
	}

	public static void writeSize2d(Size2d v, DataOutput out) {
		writeDouble(v.getHeight(), out);
		writeDouble(v.getHeight(), out);
	}

	public static TypeCode readTypeCode(DataInput in) {
		byte tc = readByte(in);
		if ( !TypeCode.isValid(tc) ) {
			throw new SerializationException("(readTypeCode) invalid typecode: " + tc);
		}
		
		return TypeCode.fromCode(tc);
	}

	public static int readNullableTypeCode(DataInput in) {
		byte tc = readByte(in);
		if ( tc >= 0 ) {
			if ( !TypeCode.isValid(tc) ) {
				throw new SerializationException("(readNullableTypeCode) invalid typecode: " + tc);
			}
		}
		else {
			if ( !TypeCode.isValid(-tc) ) {
				throw new SerializationException("(readNullableTypeCode) invalid typecode: " + -tc);
			}
		}
		
		return tc;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> T readNullableObject(DataInput in) {
		byte tc = readByte(in);
		if ( tc > 0 ) {	// valid (non-null)
			if ( !TypeCode.isValid(tc) ) {
				throw new SerializationException("(readNullableObject) invalid typecode: " + tc);
			}
			
			DataTypeSerializer serde = MarmotSerializers.getSerializer(tc);
			return (T)serde.deserialize(in);
		}
		else {
			return null;
		}
	}
	
	public static void writeGeometryColumnInfo(GeometryColumnInfo gcInfo, DataOutput out) {
		writeString(gcInfo.name(), out);
		writeString(gcInfo.srid(), out);
	}
	
	public static GeometryColumnInfo readGeometryColumnInfo(DataInput in) {
		String geomCol = readString(in);
		String srid = readString(in);
		return new GeometryColumnInfo(geomCol, srid);
	}
	
	public static void writeGRecordSchema(GRecordSchema gschema, DataOutput out) {
		gschema.getGeometryColumnInfo()
				.ifPresent(gcInfo -> {
					writeBoolean(true, out);
					writeGeometryColumnInfo(gcInfo, out);
				})
				.ifAbsent(() -> {
					writeBoolean(false, out);
				});
		RECORD_SCHEMA.serialize(gschema.getRecordSchema(), out);
	}
	
	public static GRecordSchema readGRecordSchema(DataInput in) {
		boolean flag = readBoolean(in);
		FOption<GeometryColumnInfo> ogcInfo = (flag)
											? FOption.of(readGeometryColumnInfo(in))
											: FOption.empty();
		RecordSchema schema = RECORD_SCHEMA.deserialize(in);
		
		return new GRecordSchema(ogcInfo, schema);
	}
	
	public static <T> List<T> readList(DataInput in, CheckedFunction<DataInput,T> loader) {
		int count = readVInt(in);
		if ( count >= 0 ) {
			List<T> list = new ArrayList<>(count);
			for ( int i =0; i < count; ++i ) {
				try {
					list.add(loader.apply(in));
				}
				catch ( Throwable e ) {
					throw new SerializationException("" + e);
				}
			}
			
			return list;
		}
		else {
			return null;
		}
	}
	
	public static <T extends MarmotSerializable> void writeList(Collection<T> coll, DataOutput out) {
		if ( coll != null ) {
			writeVInt(coll.size(), out);
			for ( T entry: coll ) {
				entry.serialize(out);
			}
		}
		else {
			writeVInt(-1, out);
		}
	}
	
	public static <T,S extends Collection<T>> void writeList(@Nullable S coll, DataOutput out,
														CheckedBiConsumer<T, DataOutput> elmWriter) {
		if ( coll != null ) {
			writeVInt(coll.size(), out);
			for ( T entry: coll ) {
				try {
					elmWriter.accept(entry, out);
				}
				catch ( Throwable e ) {
					throw new SerializationException("" + e);
				}
			}
		}
		else {
			writeVInt(-1, out);
		}
	}
	
	public static <T> T[] readArray(DataInput in, CheckedFunction<DataInput,T> loader, Class<T> cls) {
		int count = readVInt(in);
		@SuppressWarnings("unchecked")
		T[] array = (T[])Array.newInstance(cls, count);
		for ( int i =0; i < count; ++i ) {
			try {
				array[i] = loader.apply(in);
			}
			catch ( Throwable e ) {
				throw new SerializationException("" + e);
			}
		}
		
		return array;
	}
	
	public static <T extends MarmotSerializable> byte[] toBytes(T data) {
		Utilities.checkNotNullArgument(data, "data is null");
		
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(baos);
			data.serialize(out);
			out.flush();
			
			return baos.toByteArray();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}
	
	public static <T> byte[] toBytes(T data, CheckedConsumerX<DataOutput,IOException> writer) {
		Utilities.checkNotNullArgument(data, "data is null");
		
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(baos);
			writer.accept(out);
			out.flush();
			
			return baos.toByteArray();
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}
	
	public static <T> T fromBytes(byte[] bytes, CheckedFunctionX<DataInput,T,IOException> loader) {
		Utilities.checkNotNullArgument(bytes, "bytes is null");
		
		try {
			try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes)) ) {
				return loader.apply(dis);
			}
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static <T> T fromBytes(byte[] bytes, int offset, int length, Function<DataInput,T> loader) {
		try ( DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes, offset, length)) ) {
			return loader.apply(dis);
		}
		catch ( IOException e ) {
			throw new SerializationException("" + e);
		}
	}

	public static <T> T fromBase64String(String encoded, CheckedFunctionX<DataInput,T,IOException> loader) {
		Utilities.checkNotNullArgument(encoded, "Base64 string");
		
		return fromBytes(Base64.getDecoder().decode(encoded), loader);
	}
	
	public static <T extends MarmotSerializable> String toBase64String(T data) {
		return Base64.getEncoder().encodeToString(toBytes(data));
	}
}
