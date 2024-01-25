package marmot.dataset;

import java.util.List;

import org.locationtech.jts.geom.Envelope;

import marmot.GRecordSchema;
import marmot.RecordSchema;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.QuadClusterFile;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface DataSetX<SCAN> {
	/**
	 * 데이터세트의 식별자를 반환한다.
	 * 
	 * @return	데이터세트 식별자
	 */
	public String getId();

	/**
	 * 데이터세트의 타입을 반환한다.
	 * 
	 * @return	데이터세트 타입
	 */
	public DataSetType getType();

	/**
	 * 데이터세트의 스키마 정보를 반환한다.
	 * 
	 * @return	스키마. (공간 객체 정보 포함)
	 */
	public GRecordSchema getGRecordSchema();
	
	/**
	 * 데이터세트의 스키마 정보를 반환한다.
	 * 
	 * @return	스키마.
	 */
	public default RecordSchema getRecordSchema() {
		return getGRecordSchema().getRecordSchema();
	}
	
	/**
	 * 데이터세트의 공간 정보 여부를 반환한다.
	 * 
	 * @return	스키마.
	 */
	public default boolean hasGeometryColumn() {
		return getGRecordSchema().hasGeometryColumn();
	}

	public default GeometryColumnInfo getGeometryColumnInfo() throws NoGeometryColumnException {
		return getGRecordSchema().assertGeometryColumnInfo();
	}
	
	public default String getGeometryColumnName() throws NoGeometryColumnException {
		return getGeometryColumnInfo().name();
	}
	
	public default String getSrid() throws NoGeometryColumnException {
		return getGeometryColumnInfo().srid();
	}
	
	public Envelope getBounds() throws NoGeometryColumnException;
	
	/**
	 * 데이터세트에 저장된 레코드의 갯수를 반환한다.
	 * 
	 * @return	fpzhem rottn
	 */
	public long getRecordCount();
	
	/**
	 * 데이터세트의 저장공간 크기를 반환한다.
	 * 
	 * @return	데이터세트 크기
	 */
	public long getLength();

	/**
	 * 데이터세트에 저장된 레코드들을 읽는다.
	 * 
	 * @return	MarmotDataFrame 객체.
	 */
	public SCAN read();
	
	/**
	 * 주어진 사각 영역과 겹치는 레코드들을 읽는다.
	 * 
	 * @return	MarmotDataFrame 객체.
	 */
	public SCAN query(Envelope range) throws NoGeometryColumnException;

	/**
	 * 기본 공간 컬럼에 부여된 인덱스 등록정보를 반환한다.
	 * <p>
	 * 공간 인덱스가 생성되어 있지 않은 경우는 {@link FOption#empty()}가 반환된다.
	 * 
	 * @return	공간 인덱스 등록정보.
	 */
	public FOption<SpatialIndexInfo> getSpatialIndexInfo();
	
	public void cluster(ClusterSpatiallyOptions opts);
	
	/**
	 * 본 데이터 세트가 공간 클러스터가 존재하는지 유무를 반환한다.
	 * 
	 * @return 공간 클러스터 존재 유무
	 */
	public default boolean hasSpatialIndex() {
		return getSpatialIndexInfo().isPresent();
	}
	
	public QuadClusterFile<? extends CacheableQuadCluster> getSpatialClusterFile()
			throws NotSpatiallyClusteredException;
	
	/**
	 * 공간 클러스터가 존재하는 경우, 모든 클러스터들의 식별자(QuadKey)들을 반환한다.
	 * 
	 * @return	모든 공간 클러스터 식별자 리스트
	 */
	public List<String> getSpatialQuadKeyAll() throws NotSpatiallyClusteredException;
	
	public void delete();
}
