package marmot.io.serializer;

import java.io.DataInput;
import java.io.DataOutput;

import marmot.type.DataType;
import marmot.type.Trajectory;
import marmot.type.Trajectory.Sample;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TrajectorySerializer implements DataTypeSerializer<Trajectory> {
	@Override
	public DataType getDataType() {
		return DataType.TRAJECTORY;
	}
	
	@Override
	public void serialize(Trajectory traj, DataOutput out) {
		MarmotSerializers.writeVInt(traj.getSampleCount(), out);
		for ( Sample sample: traj.getSampleAll() ) {
			MarmotSerializers.writeDouble(sample.m_x, out);
			MarmotSerializers.writeDouble(sample.m_y, out);
			MarmotSerializers.writeLong(sample.m_ts, out);
		}
	}

	@Override
	public Trajectory deserialize(DataInput in) {
		int nsamples = MarmotSerializers.readVInt(in);
		
		Trajectory.Builder builder = Trajectory.builder();
		for ( int i =0; i < nsamples; ++i ) {
			double x = MarmotSerializers.readDouble(in);
			double y = MarmotSerializers.readDouble(in);
			long ts = MarmotSerializers.readLong(in);
			
			builder.add(new Sample(x, y, ts));
		}
		
		return builder.build();
	}
}
