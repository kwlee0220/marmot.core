package marmot.io.mapreduce.spcluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;

import marmot.GRecordSchema;
import marmot.io.HdfsPath;
import marmot.io.RecordWritable;
import marmot.io.geo.cluster.SpatialClusterFile;
import marmot.io.geo.cluster.SpatialClusterInfo;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import marmot.io.serializer.SerializationException;
import marmot.proto.GRecordSchemaProto;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialClusterInputFileFormat extends FileInputFormat<NullWritable, RecordWritable> {
	private static final String PROP_PARAMETER = "marmot.geo.cluster.parameters";
	
	public static class Parameters implements MarmotSerializable {
		public final Path m_path;					// 대상 공간 클러스터 파일의 경로명
		public final GRecordSchema m_gschema;		// 공간 클러스터의 스키마
		@Nullable public final Envelope m_range;	// 원래 데이터의 좌료계 사용
		@Nullable public final Envelope m_range84;	// EPSG4326 좌료계 사용
		
		public Parameters(Path path, GRecordSchema gschema, @Nullable Envelope range,
							@Nullable Envelope range84) {
			m_path = path;
			m_gschema = gschema;
			m_range = range;
			m_range84 = range84;
		}
		
		public static Parameters deserialize(DataInput input) {
			try {
				String path = MarmotSerializers.readString(input);
				byte[] bytes = MarmotSerializers.readBinary(input);
				GRecordSchema gschema = GRecordSchema.fromProto(GRecordSchemaProto.parseFrom(bytes));
				Envelope range = MarmotSerializers.readNullableObject(input);
				Envelope range84 = MarmotSerializers.readNullableObject(input);
				
				return new Parameters(new Path(path), gschema, range, range84);
			}
			catch ( SerializationException e ) {
				throw e;
			}
			catch ( Exception e ) {
				throw new SerializationException("fails to deserialize Parameters, cause=" + e);
			}
		}

		@Override
		public void serialize(DataOutput output) {
			MarmotSerializers.writeString(m_path.toString(), output);
			byte[] bytes = m_gschema.toProto().toByteArray();
			MarmotSerializers.writeBinary(bytes, output);
			MarmotSerializers.ENVELOPE.serializeNullable(m_range, output);
			MarmotSerializers.ENVELOPE.serializeNullable(m_range84, output);
		}
		
		@Override
		public String toString() {
			List<String> paramList = Lists.newArrayListWithCapacity(3);
			paramList.add("path=" + m_path);
			if ( m_range != null ) {
				paramList.add("range=" + m_range);
			}
			return String.format("%s[%s]", getClass().getSimpleName(),
								FStream.from(paramList).join(','));
		}
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();

		Parameters params = getParameters(conf);
		HdfsPath path = HdfsPath.of(conf, params.m_path);
		
		SpatialClusterFile scFile = SpatialClusterFile.of(path);
		
		@SuppressWarnings("resource")
		FStream<SpatialClusterInfo> infos = (params.m_range84 != null)
											? scFile.queryClusterInfos(params.m_range84)
											: scFile.getClusterInfoAll();
		List<InputSplit> splits = infos.mapOrThrow(info -> toInputSplit(path, info)).toList();
		return splits;
	}
	

	@Override
	public RecordReader<NullWritable, RecordWritable> createRecordReader(InputSplit split,
																TaskAttemptContext context)
		throws IOException, InterruptedException {
		return new SpatialClusterRecordReader();
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
	
	private static InputSplit toInputSplit(HdfsPath path, SpatialClusterInfo scInfo)
		throws IOException {
		Path clusterPath = path.child(scInfo.partitionId()).getPath();
		return new SpatialClusterFileSplit(clusterPath, scInfo);
	}
}
