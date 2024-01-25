package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.Column;
import marmot.RecordSchema;
import marmot.type.DataType;
import marmot.type.DataTypes;
import marmot.type.TypeCode;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSchemaSerializer implements MarmotSerializer<RecordSchema> {
	@Override
	public void serialize(RecordSchema schema, DataOutput out) {
		MarmotSerializers.writeShort((short)schema.getColumnCount(), out);
		for ( Column col: schema.getColumns() ) {
			MarmotSerializers.writeString(col.name(), out);
			MarmotSerializers.writeByte((byte)col.type().getTypeCode().get(), out);
		}
	}

	@Override
	public RecordSchema deserialize(DataInput in) {
		RecordSchema.Builder builder = RecordSchema.builder();
		
		short nCols = MarmotSerializers.readShort(in);
		for ( int i =0; i < nCols; ++i ) {
			String name = MarmotSerializers.readString(in);
			
			TypeCode tc = MarmotSerializers.readTypeCode(in);
			DataType type = DataTypes.fromTypeCode(tc);
			
			builder.addColumn(name, type);
		}
		
		return builder.build();
	}
}
