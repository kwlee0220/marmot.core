package marmot.io.mapreduce.spindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.locationtech.jts.geom.Envelope;

import marmot.dataset.GeometryColumnInfo;
import marmot.io.HdfsPath;
import marmot.io.RecordWritable;
import marmot.io.geo.index.GlobalIndex;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialIndexedFileInputFormat extends FileInputFormat<NullWritable, RecordWritable> {
	private static final String PROP_PARAMETER = "marmot.geo.index_query.parameters";
	
	public static class Parameters implements MarmotSerializable {
		public final GeometryColumnInfo m_gcInfo;
		@Nullable public final Envelope m_range;	// 원래 데이터의 좌료계 사용
		@Nullable public final Envelope m_range84;	// EPSG4326 좌료계 사용
		
		public Parameters(GeometryColumnInfo gcInfo, @Nullable Envelope range,
							@Nullable Envelope range84) {
			m_gcInfo = gcInfo;
			m_range = range;
			m_range84 = range84;
		}
		
		public static Parameters deserialize(DataInput input) {
			String geomCol = MarmotSerializers.readString(input);
			String srid = MarmotSerializers.readString(input);
			GeometryColumnInfo gcInfo = new GeometryColumnInfo(geomCol, srid);
			
			Envelope range = MarmotSerializers.readNullableObject(input);
			Envelope range84 = MarmotSerializers.readNullableObject(input);
			
			return new Parameters(gcInfo, range, range84);
		}

		@Override
		public void serialize(DataOutput output) {
			MarmotSerializers.writeString(m_gcInfo.name(), output);
			MarmotSerializers.writeString(m_gcInfo.srid(), output);
			MarmotSerializers.ENVELOPE.serializeNullable(m_range, output);
			MarmotSerializers.ENVELOPE.serializeNullable(m_range84, output);
		}
		
		@Override
		public String toString() {
			String rangeStr = m_range84 != null ? ": range=" + m_range84 : "";
			return String.format("%s%s]",
								getClass().getSimpleName(), rangeStr);
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		
		HdfsPath clusterDir = HdfsPath.of(conf, FileInputFormat.getInputPaths(job)[0]);
		HdfsPath globalIdxPath = GlobalIndex.toGlobalIndexPath(clusterDir);
		Parameters params = getParameters(conf);

		return GlobalIndex.open(globalIdxPath)
							.query(params.m_range84)
							.map(cidx -> toInputSplit(clusterDir, cidx))
							.toList();
	}
	
	@Override
	public RecordReader<NullWritable, RecordWritable> createRecordReader(InputSplit split,
																TaskAttemptContext context)
		throws IOException, InterruptedException {
		return new SpatialIndexedClusterRecordReader();
	}
	
	public static final Parameters getParameters(Configuration conf) {
		String encoded = conf.get(PROP_PARAMETER);
		if ( encoded == null ) {
			throw new IllegalStateException("unknown parameters: name=" + PROP_PARAMETER);
		}
		return MarmotSerializers.fromBase64String(encoded, Parameters::deserialize);
	}
	
	public static void setParameters(Configuration conf, Parameters param) {
		conf.set(PROP_PARAMETER, MarmotSerializers.toBase64String(param));
	}
	
	private static InputSplit toInputSplit(HdfsPath clusterDir, GlobalIndexEntry cidx) {
		HdfsPath packFile = clusterDir.child(cidx.packId());
		return new SpatialIndexedFileSplit(packFile.getPath(), cidx.start(),cidx.length(), cidx);
	}
}
