package marmot.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import javax.annotation.Nullable;

import org.apache.commons.text.StringSubstitutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import utils.KeyValue;
import utils.StopWatch;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.func.Lazy;
import utils.io.IOUtils;
import utils.stream.KeyValueFStream;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.support.DefaultRecord;
import marmot.support.HadoopUtils;
import marmot.support.ProgressReportable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotSequenceFile {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotSequenceFile.class);
	
	public static final String MARMOT_FILE_KEY_PREFIX = "marmot.file.";
	public static final String KEY_SCHEMA = MARMOT_FILE_KEY_PREFIX + "schema";
	public static final String KEY_GEOM_COLUMN = MARMOT_FILE_KEY_PREFIX + "geom.column";
	public static final String KEY_GEOM_SRID = MARMOT_FILE_KEY_PREFIX + "geom.srid";
	
	private final HdfsPath m_path;
	private final Lazy<FileInfo> m_info;
	
	public static MarmotSequenceFile of(HdfsPath path) {
		return new MarmotSequenceFile(path);
	}
	
	private MarmotSequenceFile(HdfsPath path) {
		Utilities.checkNotNullArgument(path, "path is null");
		
		m_path = path;
		m_info = Lazy.of(() -> loadFileInfo(m_path));
	}
	
	public Path getPath() {
		return m_path.getPath();
	}
	
	public FileSystem getFileSystem() {
		return m_path.getFileSystem();
	}
	
	public RecordSchema getRecordSchema() {
		return getFileInfo().getRecordSchema();
	}
	
	public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
		return getFileInfo().getGeometryColumnInfo();
	}
	
	public long getLength() {
		try {
			return m_path.getLength();
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public long getBlockSize() {
		try {
			return m_path.getFileStatus().getBlockSize();
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	public FileInfo getFileInfo() {
		return m_info.get();
	}
	
	public SequenceFileRecordSet read() {
		return new SequenceFileRecordSet(this);
	}
	
	public SequenceFileRecordSet read(long start, long length) {
		return new SequenceFileRecordSet(this, start, length);
	}
	
	public static Writer create(HdfsPath path, RecordSchema schema, @Nullable GeometryColumnInfo gcInfo,
								MarmotFileWriteOptions opts) {
		final Configuration conf = path.getConf();
		Map<String,String> meta = opts.metaData().orElseGet(HashMap::new);
		
		StringSubstitutor substitutor = new StringSubstitutor(meta);
		path = HdfsPath.of(conf, new Path(substitutor.replace(path.toString())));
		
		List<Option> optList = Lists.newArrayList(
										SequenceFile.Writer.file(path.getPath()),
										SequenceFile.Writer.appendIfExists(opts.appendIfExists()),
										SequenceFile.Writer.keyClass(NullWritable.class),
										SequenceFile.Writer.valueClass(RecordWritable.class));
		
		if ( !opts.appendIfExists() || !path.exists() ) {
			String codecName = opts.compressionCodecName().orElse(null);
			if ( codecName != null ) {
				CompressionCodec codec = HadoopUtils.getCompressionCodecByName(conf, codecName);
				optList.add(SequenceFile.Writer.compression(CompressionType.BLOCK, codec));
			}
			opts.blockSize().ifPresent(sz -> optList.add(SequenceFile.Writer.blockSize(sz)));
			
			// RecordSchema를 포함한 metadata 객체를 생성한다.
			Metadata metadata = toMetadata(schema, gcInfo, meta);
			optList.add(SequenceFile.Writer.metadata(metadata));
		}
		Option[] options = Iterables.toArray(optList, Option.class);
		
		try {
			SequenceFile.Writer seqWriter = SequenceFile.createWriter(conf, options);
			return new Writer(path, seqWriter);
		}
		catch ( IOException e ) {
			throw new MarmotFileException("fails to create MarmotFile: cause=" + e);
		}
	}
	public static Writer create(HdfsPath path, RecordSchema schema, @Nullable GeometryColumnInfo gcInfo) {
		return create(path, schema, gcInfo, MarmotFileWriteOptions.DEFAULT);
	}
	
	public void delete() {
		m_path.delete();
	}
	
	@Override
	public String toString() {
		return m_path.toString();
	}
	
	private static SequenceFile.Metadata
	toMetadata(RecordSchema schema, @Nullable GeometryColumnInfo gcInfo, Map<String,String> props) {
		TreeMap<Text,Text> textProps = new TreeMap<Text,Text>();
		for ( Map.Entry<String,String> propEntry: props.entrySet() ) {
			textProps.put(new Text(propEntry.getKey()), new Text(propEntry.getValue()));
		}
		textProps.put(new Text(KEY_SCHEMA), new Text(schema.toString()));
		if ( gcInfo != null ) {
			textProps.put(new Text(KEY_GEOM_COLUMN), new Text(gcInfo.name()));
			textProps.put(new Text(KEY_GEOM_SRID), new Text(gcInfo.srid()));
		};
		
		return new SequenceFile.Metadata(textProps);
	}
	
	private static FileInfo loadFileInfo(HdfsPath start) {
		Map<String,String> metadata = KeyValueFStream.from(loadMetadata(start).getMetadata())
													.mapKeyValue((k,v) -> KeyValue.of(k.toString(), v.toString()))
													.toMap();
		
		String schemaStr = metadata.remove(KEY_SCHEMA);
		if ( schemaStr == null ) {
			throw new MarmotFileException("MarmotSequenceFile does not have RecordSchema: path=" + start);
		}
		RecordSchema schema = RecordSchema.parse(schemaStr);

		String geomCol = metadata.remove(KEY_GEOM_COLUMN);
		String srid = metadata.remove(KEY_GEOM_SRID);
		FOption<GeometryColumnInfo> gcInfo = ( geomCol != null && srid != null )
											? FOption.of(new GeometryColumnInfo(geomCol, srid))
											: FOption.empty();
		return new FileInfo(schema, gcInfo, metadata);
	}
	
	private static Metadata loadMetadata(HdfsPath start) {
		try {
			HdfsPath file = start.walkRegularFileTree()
								.next()
								.getOrThrow(() -> new MarmotFileException("invalid MarmotFile: " + start));
			
			try ( SequenceFile.Reader reader = MarmotSequenceFile.of(file).readSequenceFile() ) {
				return reader.getMetadata();
			}
		}
		catch ( IOException e ) {
			throw new MarmotFileException(e);
		}
	}
	
	SequenceFile.Reader readSequenceFile() throws IOException {
		return new SequenceFile.Reader(m_path.getConf(), SequenceFile.Reader.file(m_path.getPath()));
	}
	
	public static class FileInfo {
		private final RecordSchema m_schema;
		private final FOption<GeometryColumnInfo> m_gcInfo;
		private final Map<String,String> m_meta;
		
		private FileInfo(RecordSchema schema, FOption<GeometryColumnInfo> gcInfo,
						Map<String,String> metadata) {
			m_schema = schema;
			m_gcInfo = gcInfo;
			m_meta = Maps.newHashMap(metadata);
		}
		
		public RecordSchema getRecordSchema() {
			return m_schema;
		}
		
		public FOption<GeometryColumnInfo> getGeometryColumnInfo() {
			return m_gcInfo;
		}
		
		public Map<String,String> getMetadata() {
			return Collections.unmodifiableMap(m_meta);
		}
	}
	
	public static class Writer implements Closeable {
		private final HdfsPath m_path;
		private final SequenceFile.Writer m_writer;
		
		private long m_length = -1;	// close 여부를 판단에 사용
		
		private Writer(HdfsPath path, SequenceFile.Writer seqWriter) {
			m_path = path;
			m_writer = seqWriter;
		}

		@Override
		public void close() {
			if ( m_length < 0 ) {
				try {
					m_length = m_writer.getLength();
					IOUtils.closeQuietly(m_writer);
				}
				catch ( IOException e ) {
					throw new MarmotFileException(e);
				}
			}
		}
		
		public boolean isClosed() {
			return m_length >= 0;
		}
		
		public long getLength() {
			try {
				return m_length >= 0 ? m_length : m_writer.getLength();
			}
			catch ( IOException e ) {
				throw new MarmotFileException("fails to get file length", e);
			}
		}
		
		public void write(RecordWritable value) {
			try {
				m_writer.append(NullWritable.get(), value);
			}
			catch ( Throwable e ) {
				throw new MarmotFileException("fails to write a record into file=" + m_path + ", cause=" + e);
			}
		}

		public void write(Record record) {
			write(RecordWritable.from(record));
		}
		
		@Override
		public String toString() {
			return String.format("MarmotSequenceFile.Writer: path=%s, length=%s]",
								m_path, UnitUtils.toByteSizeString(getLength()));
		}
	}

	public static Store store(HdfsPath path, RecordSet rset,
											@Nullable GeometryColumnInfo gcInfo,
											MarmotFileWriteOptions opts) {
		return new Store(path, rset, gcInfo, opts);
	}
	
	public static class Store implements Callable<DataSetPartitionInfo>, ProgressReportable {
		private final HdfsPath m_path;
		private final RecordSet m_rset;
		private final RecordSchema m_schema;
		private final MarmotFileWriteOptions m_opts;
		private String m_optorName = "MarmotSequenceFile.Store";
		
		private final @Nullable GeometryColumnInfo m_gcInfo;
		private final int m_geomColIdx;
		
		private Envelope m_bounds = new Envelope();
		private long m_count = 0;
		private long m_size;
		private volatile boolean m_isClosed = false;
		
		private volatile Writer m_writer = null;
		protected long m_elapsed;
		private boolean m_finalProgressReported = false;
		
		Store(HdfsPath path, RecordSet rset, @Nullable GeometryColumnInfo gcInfo,
				MarmotFileWriteOptions opts) {
			m_path = path;
			m_rset = rset;
			m_schema = rset.getRecordSchema();
			m_opts = opts;

			m_gcInfo = gcInfo;
			if ( m_gcInfo != null ) {
				m_geomColIdx = m_schema.findColumn(m_gcInfo.name())
									.map(Column::ordinal)
									.getOrThrow(() -> new IllegalArgumentException("invalid Geometry column: col=" + m_gcInfo.name()));
			}
			else {
				m_geomColIdx = -1;
			}
		}
		
		public DataSetPartitionInfo getDataSetPartitionInfo() {
			return new DataSetPartitionInfo(m_bounds, m_count, m_size);
		}
		
		public void setOperatorName(String optorName) {
			m_optorName = optorName;
		}

		@Override
		public DataSetPartitionInfo call() {
			Utilities.checkState(!m_isClosed, "closed already");

			m_count = 0;
			m_writer = MarmotSequenceFile.create(m_path, m_schema, m_gcInfo, m_opts);
			try {
				Record record = DefaultRecord.of(m_schema);
				while ( m_rset.next(record) ) {
					m_writer.write(record);
					if ( m_geomColIdx >= 0 ) {
						Geometry geom = record.getGeometry(m_geomColIdx);
						if ( geom != null && !geom.isEmpty() ) {
							m_bounds.expandToInclude(geom.getEnvelopeInternal());
						}
					}
					++m_count;
				}
				
				return new DataSetPartitionInfo(m_bounds, m_count, m_writer.getLength());
			}
			finally {
				IOUtils.closeQuietly(m_writer);
				m_writer = null;
				
				m_rset.closeQuietly();
				m_isClosed = true;
			}
		}
		
		@Override
		public void reportProgress(Logger logger, StopWatch elapsed) {
			if ( !m_isClosed || !m_finalProgressReported ) {
				if ( m_rset instanceof ProgressReportable ) {
					((ProgressReportable)m_rset).reportProgress(logger, elapsed);
				}
				
				m_elapsed = elapsed.getElapsedInMillis();
				logger.info("report: [{}]{}", m_isClosed ? "C": "O", toString());
				
				if ( m_isClosed ) {
					m_finalProgressReported = true;
				}
			}
		}
		
		@Override
		public String toString() {
			if ( m_elapsed > 0 && m_writer != null ) {
				m_size = m_writer.getLength();
				String sizeStr = UnitUtils.toByteSizeString(m_size);
				long veloSize = Math.round(((double)m_size / m_elapsed) * 1000);
				String veloSzStr = UnitUtils.toByteSizeString(veloSize);
				long veloCnt = Math.round(((double)m_count / m_elapsed) * 1000);
				return String.format("%s: path=%s, count=%d, size=%s, velo=%s(%,d)/s",
										m_optorName, m_path, m_count, sizeStr, veloSzStr, veloCnt);
			}
			else {
				String sizeStr = UnitUtils.toByteSizeString(m_size);
				return String.format("%s: path=%s, count=%,d, size=%s",
									m_optorName, m_path, m_count, sizeStr);
			}
		}
	}
}
