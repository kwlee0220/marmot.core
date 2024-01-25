package marmot.io.mapreduce.seqfile;

import static marmot.io.MarmotFileWriteOptions.META_DATA;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.HdfsPath;
import marmot.io.MarmotSequenceFile;
import marmot.io.RecordWritable;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileOutputFormat
						extends SequenceFileOutputFormat<NullWritable, RecordWritable> {
	@Override
	public MarmotRecordWriter getRecordWriter(TaskAttemptContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		Map<String,String> props = conf.getValByRegex(MarmotSequenceFile.MARMOT_FILE_KEY_PREFIX + "*");
		HdfsPath path = HdfsPath.of(conf, getDefaultWorkFile(context, ""));
		GeometryColumnInfo gcInfo = getGeometryColumnInfo(conf).getOrNull();
		MarmotSequenceFile.Writer writer = MarmotSequenceFile.create(path, getRecordSchema(conf),
																	gcInfo, META_DATA(props));
		return new MarmotRecordWriter(writer);
	}
	
	public static class MarmotRecordWriter extends RecordWriter<NullWritable, RecordWritable> {
		private MarmotSequenceFile.Writer m_writer;
		
		MarmotRecordWriter(MarmotSequenceFile.Writer writer) {
			m_writer = writer;
		}
		
		@Override
		public void write(NullWritable key, RecordWritable record) throws IOException, InterruptedException {
			m_writer.write(record);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			m_writer.close();
		}
	}
	
	public static final RecordSchema getRecordSchema(Configuration conf) throws IOException {
		String schemaStr = conf.get(MarmotSequenceFile.KEY_SCHEMA);
		if ( schemaStr == null ) {
			throw new RecordSetException("RecordSchema descriptor is not present at Configuration: "
											+ "property=" + MarmotSequenceFile.KEY_SCHEMA);
		}
		
		return RecordSchema.parse(schemaStr);
	}
	
	public static final void setRecordSchema(Configuration conf, RecordSchema schema) {
		conf.set(MarmotSequenceFile.KEY_SCHEMA, schema.toString());
	}
	
	public static final FOption<GeometryColumnInfo> getGeometryColumnInfo(Configuration conf)
		throws IOException {
		String geomCol = conf.get(MarmotSequenceFile.KEY_GEOM_COLUMN);
		String srid = conf.get(MarmotSequenceFile.KEY_GEOM_SRID);
		return ( geomCol != null && srid != null )
				? FOption.of(new GeometryColumnInfo(geomCol, srid))
				: FOption.empty();
	}
	
	public static final void setGeometryColumnInfo(Configuration conf, GeometryColumnInfo gcInfo) {
		conf.set(MarmotSequenceFile.KEY_GEOM_COLUMN, gcInfo.name());
		conf.set(MarmotSequenceFile.KEY_GEOM_SRID, gcInfo.srid());
	}
}
