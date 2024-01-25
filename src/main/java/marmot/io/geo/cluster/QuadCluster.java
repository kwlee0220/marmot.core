package marmot.io.geo.cluster;

import org.locationtech.jts.geom.Envelope;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.geo.join.SpatialLookupTable;
import marmot.support.EnvelopeTaggedRecord;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface QuadCluster extends SpatialLookupTable {
	/**
	 * 클러스터에 식별자인 QuadKey를 반환한다.
	 * 
	 * @return	클러스터 식별자(QuadKey)
	 */
	public String getQuadKey();
	
	/**
	 * 클러스터에 저장된 레코드의 스키마를 반환한다.
	 * 
	 * @return	레코드 스키마.
	 */
	public GRecordSchema getGRecordSchema();
	
	public default RecordSchema getRecordSchema() {
		return getGRecordSchema().getRecordSchema();
	}
	
	/**
	 * 클러스터의 quadspace 영역의 Envelope를 반환한다.
	 * 반환되는 Envelope의 좌표계는 반드시 EPSG:4326이어야 한다.
	 * 
	 * @return Envelope
	 */
	public Envelope getQuadBounds();
	
	/**
	 * 클러스터에 저장된 전체 레코드의 MBR을 반환한다.
	 * 
	 * @return	Envelope
	 */
	public Envelope getDataBounds();
	
	/**
	 * 클러스터에 저장된 레코드의 갯수를 반환한다.
	 * 
	 * @return	레코드 수.
	 */
	public long getRecordCount();
	
	/**
	 * 클러스터에 저장된 레코드 중에서 본 공간의 소유가 아닌 레코드의 갯수를 반환한다.
	 * 
	 * @return	레코드 수
	 */
	public long getDuplicateCount();
	
	/**
	 * 클러스터에 저장된 레코드들 중에서 클러스터 소유의 레코드 갯수를 반환한다.
	 * 
	 * @return	레코드 수.
	 */
	public default long getOwnedRecordCount() {
		return getRecordCount() - getDuplicateCount();
	}
	
	/**
	 * 클러스터에 포함된 모든 레코드들을 접근하는 스트림을 반환한다.
	 * 
	 * @param dropDuplicates		복제본 제외 여부
	 * @return 레코드 스트림.
	 */
	public FStream<EnvelopeTaggedRecord> read(boolean dropDuplicates);
	
	/**
	 * 클러스터에 포함된 모든 레코드에 대해 주어진 키와 겹치는 레코드 스트림을 반환한다.
	 * 
	 * @param range84			질의 영역, 위경도(WGS84) 좌표계 사용
	 * @param dropDuplicates	복제본 제외 여부
	 * @return	질의에 포함된 레코드들의 스트림.
	 */
	public FStream<EnvelopeTaggedRecord> query(Envelope range84, boolean dropDuplicates);

	public FStream<Record> queryRecord(Envelope range, boolean dropDuplicates);
}