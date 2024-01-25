package marmot.dataset;

import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class SpatialIndexCatalogInfo {
	private String m_dsId;
	private GeometryColumnInfo m_geomCol;
	private String m_hdfsPath;
	private long m_updatedMillis = -1;
	
	public SpatialIndexCatalogInfo(String dsId, GeometryColumnInfo geomCol, String hdfsPath) {
		Utilities.checkNotNullArgument(dsId, "dataset is null");
		Utilities.checkNotNullArgument(geomCol, "geomCol is null");
		Utilities.checkNotNullArgument(hdfsPath, "hdfsPath is null");
		
		m_dsId = dsId;
		m_geomCol = geomCol;
		m_hdfsPath = hdfsPath;
		m_updatedMillis = System.currentTimeMillis();
	}
	
	/**
	 * 공간 인덱스가 생성된 대상 데이터 세트 식별자를 반환한다.
	 * 
	 * @return	데이터 세트 식별자
	 */
	public String getDataSetId() {
		return m_dsId;
	}
	
	/**
	 * 공간 인덱스 대상 공간 컬럼 정보를 반환한다.
	 * 
	 * @return	공간 컬럼 정보
	 */
	public GeometryColumnInfo getGeometryColumnInfo() {
		return m_geomCol;
	}
	
	public String getHdfsFilePath() {
		return m_hdfsPath;
	}
	
	public long getUpdatedMillis() {
		return m_updatedMillis;
	}
	
	public void setUpdatedMillis(long millis) {
		m_updatedMillis = millis;
	}
}
