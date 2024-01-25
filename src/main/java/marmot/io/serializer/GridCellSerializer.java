package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;
import marmot.type.GridCell;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GridCellSerializer implements ComparableMarmotSerDe<GridCell> {
	@Override
	public DataType getDataType() {
		return DataType.GRID_CELL;
	}
	
	@Override
	public void serialize(GridCell cell, DataOutput out) {
		MarmotSerializers.writeInt(cell.getRowIdx(), out);
		MarmotSerializers.writeInt(cell.getColIdx(), out);
	}

	@Override
	public GridCell deserialize(DataInput in) {
		int row = MarmotSerializers.readInt(in);
		int col = MarmotSerializers.readInt(in);
		
		return new GridCell(col, row);
	}

	@Override
	public int compareBytes(Cursor cursor) {
		int cmp1 = cursor.compareInt();
		int cmp2 = cursor.compareInt();
		
		if ( cmp1 == 0 ) {
			return cmp2;
		}
		else {
			return cmp1;
		}
	}
}
