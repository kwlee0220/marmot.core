package marmot.dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

import com.google.common.base.Preconditions;

import utils.Utilities;
import utils.func.CheckedFunctionX;
import utils.func.FOption;
import utils.func.Try;
import utils.io.IOUtils;
import utils.jdbc.JdbcException;
import utils.jdbc.JdbcUtils;

import marmot.RecordSchema;
import marmot.geo.catalog.CatalogException;
import marmot.geo.catalog.DataSetInfo;
import marmot.io.DataSetPartitionInfo;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Catalog {
	private static final String PROP_DEF_DATASET_DIR = "marmot.catalog.heap.dir";
	private static final String PROP_DEF_SPATIAL_INDEX_DIR = "marmot.catalog.spatial_indexes.dir";
	private static final Path DEF_DATASET_DIR = new Path("database/heap");
	private static final Path DEF_INDEX_DIR = new Path("database/spatial_indexes");

	private final Configuration m_conf;
	private final Path m_datasetsDir;
	private final Path m_indexesDir;
	
	public static Catalog initialize(Configuration conf) {
		String value;
		value = conf.get(PROP_DEF_DATASET_DIR, null);
		Path dsDir = (value != null) ? new Path(value) : DEF_DATASET_DIR;
		
		value = conf.get(PROP_DEF_SPATIAL_INDEX_DIR, null);
		Path indexesDir = (value != null) ? new Path(value) : DEF_INDEX_DIR;
		
		return new Catalog(conf, dsDir, indexesDir);
	}
	
	private Catalog(Configuration conf, Path datasetsDir, Path indexesDir) {
		m_conf = conf;
		m_datasetsDir = datasetsDir;
		m_indexesDir = indexesDir;
	}
	
	public Configuration getConfiguration() {
		return m_conf;
	}
	
	public void initialize(DataSetInfo src) {
		String name = Catalogs.normalize(src.getId());
		
		src.setDirName(getParentDir(name));
		String path = src.getFilePath();
		if ( path == null ) {
			src.setFilePath(generateFilePath(name).toString());
		}
	}
	
	public void insertDataSetInfo(DataSetInfo info) {
		Utilities.checkNotNullArgument(info, "DataSetInfo should not be null.");

		String id = Catalogs.normalize(info.getId());
		try ( Connection conn = getConnection(m_conf) ) {
			// 주어진 경로명과 동일하거나, 앞은 동일하면서 더 긴 경로명의 데이터세트가 있는지
			// 조사한다.
			try ( PreparedStatement pstmt = conn.prepareStatement(SQL_EXISTS_DATASET) ) {
				pstmt.setString(1, id);
				pstmt.setString(2, id + "/%");
				ResultSet rs = pstmt.executeQuery();
				if ( rs.next() ) {
					throw new DataSetExistsException(id);
				}
			}
			
			if ( isDirectoryInGuard(conn, id) ) {
				throw new DataSetExistsException("directory=" + id);
			}
			
			try ( PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT_DATASET) ) {
				String geomCol = info.getGeometryColumnInfo()
										.map(i -> i.name())
										.orElse("");
				String srid = info.getGeometryColumnInfo()
									.map(GeometryColumnInfo::srid)
									.orElse("");
			
				pstmt.setString(1, id);
				pstmt.setString(2, getParentDir(id));
				pstmt.setString(3, info.getType().id());
				pstmt.setString(5, geomCol);
				pstmt.setString(4, srid);
				pstmt.setString(6, fromEnvelope(info.getBounds())); 
				pstmt.setLong(7, info.getRecordCount());
				pstmt.setString(8, info.getRecordSchema().toString());
				
				String path = info.getFilePath();
				if ( path == null ) {
					path = generateFilePath(id).toString();
				}
				pstmt.setString(9, path);
				
				pstmt.setString(10, info.getCompressionCodecName().orElse(null));
				pstmt.setLong(11, info.getBlockSize());
				pstmt.setFloat(12, info.getThumbnailRatio());
				pstmt.setLong(13, info.getUpdatedMillis());
				
				if ( pstmt.executeUpdate() <= 0 ) {
					throw new CatalogException("fails to create DataSet");
				}
			}
		}
		catch ( SQLException e ) {
			String state = e.getSQLState();
			if ( state.equals("23505") ) {
				throw new DataSetExistsException(id, e);
			}
			
			throw new CatalogException(e);
		}
	}
	
	public void insertOrReplaceDataSetInfo(DataSetInfo info) {
		deleteDataSetInfo(info.getId());
		insertDataSetInfo(info);
	}

	/**
	 * 주어진 이름에 해당하는 데이터세트 정보를 반환한다.
	 * 
	 * @param dsId	데이터세트 이름
	 * @return	데이터세트 정보 객체.
	 * 			이름에 해당하는 데이터세트가 등록되지 않은 경우는
	 * 			{@code Option.none()}을 반환함.
	 * @throws CatalogException			카타로그 정보 접근 중 오류가 발생된 경우.
	 */
	public FOption<DataSetInfo> getDataSetInfo(String dsId) {
		dsId = Catalogs.normalize(dsId);
		
		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_DATASET); ) {
			pstmt.setString(1, dsId);
			
			return FOption.from(JdbcUtils.stream(pstmt.executeQuery(), s_toDsInfo)
												.findAny());
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}
	
	public List<DataSetInfo> getDataSetInfoAll() {
		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_DATASET_ALL); ) {
			return JdbcUtils.stream(pstmt.executeQuery(), s_toDsInfo)
							.collect(Collectors.toList());
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}
	
	public boolean isDirectory(String id) {
		id = Catalogs.normalize(id);

		try ( Connection conn = getConnection(m_conf) ) {
			return isDirectoryInGuard(conn, id);
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}
	
	private boolean isDirectoryInGuard(Connection conn, String id) throws SQLException {
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_IS_FOLDER) ) {
			pstmt.setString(1, id);
	
			return JdbcUtils.stream(pstmt.executeQuery(), s_toCount).findAny().get() > 0;
		}
	}
	
	public List<DataSetInfo> getDataSetInfoAllInDir(String folder, boolean recursive) {
		folder = Catalogs.normalize(folder);

		try ( Connection conn = getConnection(m_conf) ) {
			PreparedStatement pstmt;
			if ( recursive ) {
				pstmt = conn.prepareStatement(SQL_LIST_DATASETS_AT_FOLDER_RECURSIVE);
				pstmt.setString(1, folder);
				pstmt.setString(2, folder + "/%");
			}
			else {
				pstmt = conn.prepareStatement(SQL_LIST_DATASETS_AT_FOLDER);
				pstmt.setString(1, folder);
			}
			
			return JdbcUtils.stream(pstmt.executeQuery(), s_toDsInfo)
							.collect(Collectors.toList());
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}
	
	public boolean deleteDataSetInfo(String id) {
		id = Catalogs.normalize(id);

		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_DATASET); ) {
			pstmt.setString(1, id);
			return pstmt.executeUpdate() > 0;
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}

	public List<String> getDirAll() {
		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_FOLDER_ALL); ) {
			return JdbcUtils.stream(pstmt.executeQuery(), s_toFolder)
							.collect(Collectors.toList());
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}

	public String getParentDir(String folder) {
		folder = Catalogs.normalize(folder);
		return Catalogs.getFolder(folder);
	}

	public List<String> getSubDirAll(String folder, boolean recursive) {
		String prefix = Catalogs.normalize(folder);
		if ( !prefix.endsWith("/") ) {
			prefix = prefix + "/";
		}
		int prefixLen = prefix.length();

		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_SUBFOLDER_ALL); ) {
			pstmt.setString(1, prefix);
			pstmt.setString(2, prefix + "%");
			
			Stream<String> folderStrm = JdbcUtils.stream(pstmt.executeQuery(), s_toFolder);
			if ( !recursive ) {
				folderStrm = folderStrm.map(name -> name.substring(prefixLen))
										.map(name -> {
											int idx = name.indexOf(Catalogs.ID_DELIM);
											if ( idx >= 0 ) {
												name = name.substring(0, idx);
											}
											return name;
										})
										.distinct();
			}
			return folderStrm.collect(Collectors.toList());
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}
	
	public int deleteDir(String folder) {
		folder = Catalogs.normalize(folder);

		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_FOLDER); ) {
			pstmt.setString(1, folder);
			pstmt.setString(2, folder + "/%");
			
			return pstmt.executeUpdate();
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}
	
	public void insertSpatialIndexCatalogInfo(SpatialIndexCatalogInfo info) {
		Preconditions.checkArgument(info != null, "SpatialIndexInfo should not be null.");

		String name = Catalogs.normalize(info.getDataSetId());
		try ( Connection conn = getConnection(m_conf);
				PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT_SPATIAL_INDEX); ) {
			pstmt.setString(1, name);
			pstmt.setString(2, info.getGeometryColumnInfo().name());
			pstmt.setString(3, info.getGeometryColumnInfo().srid());
			pstmt.setString(4, info.getHdfsFilePath());
			pstmt.setLong(5, info.getUpdatedMillis());
			
			if ( pstmt.executeUpdate() <= 0 ) {
				throw new CatalogException("fails to insert SpatialIndexInfo");
			}
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}
	
	public Optional<SpatialIndexCatalogInfo> getSpatialIndexCatalogInfo(String dsId) {
		dsId = Catalogs.normalize(dsId);
		
		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_SPATIAL_INDEX1); ) {
			pstmt.setString(1, dsId);
			
			return JdbcUtils.fstream(pstmt.executeQuery(), s_toSIInfo).next().toOptional();
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}
	
	public boolean deleteSpatialIndexCatalogInfo(String dsId, String geomCol) {
		String id = Catalogs.normalize(dsId);

		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_SPATIAL_INDEX); ) {
			pstmt.setString(1, id);
			pstmt.setString(2, geomCol);
			return pstmt.executeUpdate() > 0;
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	}
	
	public Path generateFilePath(String dsId) {
		dsId = Catalogs.normalize(dsId).substring(1);
		return new Path(m_datasetsDir, dsId);
	}
	
	public Path generateSpatialIndexPath(String dsId, String geomCol) {
		dsId = Catalogs.normalize(dsId);
		String suffix = new Path(dsId, geomCol).toString().substring(1);
		return new Path(m_indexesDir, suffix);
	}
	
	public boolean isExternalDataSet(String path) {
		return !path.startsWith(m_datasetsDir.toString());
	}
	
	public void updateDataSetInfo(String dsId, DataSetPartitionInfo dspInfo) {
		FOption<DataSetInfo> odsInfo = getDataSetInfo(dsId);
		if ( odsInfo.isPresent() && dspInfo.count() > 0 ) {
			DataSetInfo dsInfo = odsInfo.get();

			dsInfo.getBounds().expandToInclude(dspInfo.bounds());
			dsInfo.setRecordCount(dsInfo.getRecordCount() + dspInfo.count());
			
			insertOrReplaceDataSetInfo(dsInfo);
		}
	}
	
	private static final String SQL_EXISTS_DATASET
		= "select name from datasets where name = ? or name like ?";
	
	private static final String SQL_GET_DATASET_ALL
		= "select name, folder, type, srid, geom_column, envelope, count, schema, "
		+ "hdfs_path, compression_codec_name, block_size, thumbnail_ratio, updated_millis "
		+ "from datasets";

	private static final String SQL_GET_DATASET
		= "select name, folder, type, srid, geom_column, envelope, count, schema, "
		+ "hdfs_path, compression_codec_name, block_size, thumbnail_ratio, updated_millis "
		+ "from datasets where name=?";
	
	private static final String SQL_IS_FOLDER
		= "select count(*) from datasets where folder=?";
	
	private static final String SQL_LIST_DATASETS_AT_FOLDER
		= "select name, folder, type, srid, geom_column, envelope, count, schema, "
		+ "hdfs_path, compression_codec_name, block_size, thumbnail_ratio, updated_millis "
		+ "from datasets where folder=?";
	
	private static final String SQL_LIST_DATASETS_AT_FOLDER_RECURSIVE
		= "select name, folder, type, srid, geom_column, envelope, count, schema, "
		+ "hdfs_path, compression_codec_name, block_size, thumbnail_ratio, updated_millis "
		+ "from datasets where folder = ? or folder LIKE ?";
	
	private static final String SQL_INSERT_DATASET
		= "insert into datasets "
		+	"(name, folder, type, srid, geom_column, envelope, count, schema, "
		+ 		"hdfs_path, compression_codec_name, block_size, thumbnail_ratio, updated_millis) "
		+	"values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static final String SQL_DELETE_DATASET = "delete from datasets where name = ?";
	
	private static final String SQL_GET_FOLDER_ALL
		= "select distinct(folder) from datasets where folder <> ''";
	
	private static final String SQL_GET_SUBFOLDER_ALL
		= "select distinct(folder) from datasets where folder <> ? and folder LIKE ?";
	
	private static final String SQL_DELETE_FOLDER = "delete from datasets "
												+ "where folder = ? or folder like ?";
	
	private static final String SQL_CREATE_DATASETS
		= "create table datasets ("
		+ 	"name varchar not null,"
		+ 	"folder varchar not null,"
		+ 	"type varchar not null,"
		+ 	"srid varchar not null,"
		+ 	"geom_column varchar not null,"
		+ 	"envelope varchar not null,"
		+ 	"count bigint not null,"
		+ 	"schema varchar not null,"
		+ 	"hdfs_path varchar not null,"
		+ 	"compression_codec_name varchar,"
		+ 	"block_size bigint not null,"
		+ 	"thumbnail_ratio float not null,"
		+ 	"updated_millis bigint not null,"
		+ 	"primary key (name)"
		+ ")";

	private static final String SQL_CREATE_SPATIAL_INDEXES
		= "create table spatial_indexes ("
		+ 	"dataset varchar not null,"
		+ 	"geom_column varchar not null,"
		+ 	"srid varchar not null,"
		+ 	"hdfs_path varchar not null,"
		+ 	"updated_millis bigint not null,"
		+ 	"primary key (dataset, geom_column)"
		+ ")";

	private static final String SQL_INSERT_SPATIAL_INDEX
		= "insert into spatial_indexes "
		+	"(dataset, geom_column, srid, hdfs_path, updated_millis) "
		+	"values (?,?,?,?,?)";

	private static final String SQL_GET_SPATIAL_INDEX1
		= "select dataset, idx.geom_column, idx.srid, idx.hdfs_path, idx.updated_millis "
		+ "from datasets ds, spatial_indexes idx "
		+ "where ds.name=? and ds.name = idx.dataset and idx.geom_column=ds.geom_column ";

	private static final String SQL_GET_SPATIAL_INDEX2
		= "select dataset, geom_column, srid, hdfs_path, updated_millis "
		+ "from spatial_indexes where dataset=? and geom_column=?";
	
	private static final String SQL_DELETE_SPATIAL_INDEX
						= "delete from spatial_indexes where dataset = ? and geom_column = ?";
	
	public static Catalog createCatalog(Configuration conf) {
		Connection conn = null;
		try {
			conn = getConnection(conf);
			
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(SQL_CREATE_DATASETS);
			stmt.executeUpdate(SQL_CREATE_SPATIAL_INDEXES);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
		finally {
			IOUtils.closeQuietly(conn);
		}
		
		return initialize(conf);
	}
	
	public static void dropCatalog(Configuration conf) {
		Connection conn = null;
		try {
			conn = getConnection(conf);
			
			final Statement stmt = conn.createStatement();
			Try.run(()->stmt.executeUpdate("drop table datasets"));
			Try.run(()->stmt.executeUpdate("drop table spatial_indexes"));
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
		finally {
			IOUtils.closeQuietly(conn);
		}
	}
	
	private static Connection getConnection(Configuration conf) {
		String jdbcUrl = conf.get("marmot.catalog.jdbc.url");
		if ( jdbcUrl == null ) {
			throw new CatalogException("fails to get JDBC url: name=marmot.catalog.jdbc.url");
		}
		String user = conf.get("marmot.catalog.jdbc.user");
		if ( user == null ) {
			throw new CatalogException("fails to get JDBC user: name=marmot.catalog.jdbc.user");
		}
		String passwd = conf.get("marmot.catalog.jdbc.passwd");
		if ( passwd == null ) {
			throw new CatalogException("fails to get JDBC user: name=marmot.catalog.jdbc.passwd");
		}
		String driverClassName = conf.get("marmot.catalog.jdbc.driver");
		if ( driverClassName == null ) {
			throw new CatalogException("fails to get JDBC driver class: name=marmot.catalog.jdbc.driver");
		}
		
		try {
			Class.forName(driverClassName);
			return DriverManager.getConnection(jdbcUrl, user, passwd);
		}
		catch ( Exception e ) {
			throw new CatalogException(e);
		}
	}
	
	private static final CheckedFunctionX<ResultSet,DataSetInfo,SQLException> s_toDsInfo = rs -> {
		try {
			String name = rs.getString(1);
			DataSetType type = DataSetType.fromString(rs.getString(3));
			RecordSchema schema = RecordSchema.parse(rs.getString(8));
			
			DataSetInfo info = new DataSetInfo(name, type, schema);
			info.setDirName(rs.getString(2));
			String geomCol = rs.getString(5);
			if ( geomCol.length() > 0 ) {
				String srid = rs.getString(4);
				GeometryColumnInfo geomColInfo = new GeometryColumnInfo(geomCol, srid);
				info.setGeometryColumnInfo(Optional.of(geomColInfo));
			}
			info.setBounds(toEnvelope(rs.getString(6)));
			info.setRecordCount(rs.getLong(7));
			info.setFilePath(rs.getString(9));
			info.setCompressionCodecName(rs.getString(10));
			info.setBlockSize(rs.getLong(11));
			info.setThumbnailRatio(rs.getFloat(12));
			info.setUpdatedMillis(rs.getLong(13));
			
			return info;
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	};
	
	private static final CheckedFunctionX<ResultSet,SpatialIndexCatalogInfo,SQLException> s_toSIInfo = rs -> {
		try {
			String dsId = rs.getString(1);
			GeometryColumnInfo gcInfo = new GeometryColumnInfo(rs.getString(2),  rs.getString(3));
			String path = rs.getString(4);
			SpatialIndexCatalogInfo info = new SpatialIndexCatalogInfo(dsId, gcInfo, path);
			info.setUpdatedMillis(rs.getLong(5));
			
			return info;
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	};
	
	private static final Envelope toEnvelope(String envlStr) {
		if ( envlStr.length() > 0 ) {
			double[] v = Stream.of(envlStr.split(";"))
								.mapToDouble(Double::parseDouble)
								.toArray();
			return new Envelope(new Coordinate(v[0], v[1]), new Coordinate(v[2], v[3]));
		}
		else {
			return new Envelope();
		}
	}
	
	private static final String fromEnvelope(Envelope envl) {
		if ( !envl.isNull() ) {
			return Stream.of(envl.getMinX(),envl.getMinY(), envl.getMaxX(),envl.getMaxY())
						.map(Object::toString)
						.collect(Collectors.joining(";"));
		}
		else {
			return "";
		}
	}
	
	private static final CheckedFunctionX<ResultSet,String,SQLException> s_toFolder = rs -> {
		try {
			return rs.getString(1);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	};
	
	private static final CheckedFunctionX<ResultSet,Integer,SQLException> s_toCount = rs -> {
		try {
			return rs.getInt(1);
		}
		catch ( SQLException e ) {
			throw new CatalogException(e);
		}
	};
}
