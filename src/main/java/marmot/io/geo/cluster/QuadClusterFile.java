package marmot.io.geo.cluster;

import java.util.Set;

import org.locationtech.jts.geom.Envelope;

import marmot.GRecordSchema;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.Match;
import marmot.support.EnvelopeTaggedRecord;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface QuadClusterFile<T extends QuadCluster> {
	/**
	 * 공간 클러스터 데이터세트에 저장된 레코드들의 스키마를 반환한다.
	 * 
	 * @return	레코드 스키마
	 */
	public GRecordSchema getGRecordSchema();
	
	/**
	 * 공간 클러스터 데이터세트에 저장된 레코드들의 스키마를 반환한다.
	 * 
	 * @return	레코드 스키마
	 */
	public default RecordSchema getRecordSchema() {
		return getGRecordSchema().getRecordSchema();
	}
	
	/**
	 * 공간 클러스터 파일에 포함된 모든 데이타의 MBR 영역을 반환한다.
	 * 
	 * @return	MBR 영역
	 */
	public Envelope getDataBounds();
	
	/**
	 * 공간 클러스터 파일에 포함된 파티션의 전체의 MBR 영역을 반환한다.
	 * 
	 * @return	MBR 영역
	 */
	public Envelope getQuadBounds();

	/**
	 * 공간 클러스터 파일에 저장된 레코드의 갯수를 반환한다.
	 * 
	 * @return	레코드 수.
	 */
	public long getRecordCount();
	
	/**
	 * 공간 클러스터 파일에 저장된 레코드들 중에서 중복된 레코드  갯수를 반환한다.
	 * 
	 * @return	레코드 수.
	 */
	public long getDuplicateCount();
	
	/**
	 * 공간 클러스터 파일에 저장된 레코드들 중에서 클러스터 소유의 레코드 갯수를 반환한다.
	 * 
	 * @return	레코드 수.
	 */
	public default long getOwnedRecordCount() {
		return getRecordCount() - getDuplicateCount();
	}

	/**
	 * 공간 클러스터 파일에 저장된 모든 레코드들을 반환한다.
	 * 
	 * @return	공간 레코드 스트림. 
	 */
	public default FStream<Record> read() {
		return FStream.from(getClusterKeyAll())
						.flatMap(qk -> getCluster(qk).read(true))
						.map(EnvelopeTaggedRecord::getRecord);
	}
	
	/**
	 * 공간 클러스터 파일에 포함된 클러스터의 갯수를 반환한다.
	 * 
	 * @return	파티션 갯수
	 */
	public int getClusterCount();

	/**
	 * 저장된 모든 클러스터들의 식별자들을 반환한다.
	 * 
	 * @return	클러스터들의 식별자 리스트.
	 */
	public Set<String> getClusterKeyAll();
	
	/**
	 * 주어진 식별자(quadKey)에 해당하는 클러스터 객체를 반환한다.
	 * 
	 * @param quadKey	대상 클러스터의 quad-key.
	 * @return	적재된 클러스터 객체.
	 */
	public T getCluster(String quadKey);

	/**
	 * 주어진 사각 영역과 겹치는 모든 클러스터들의 식별자를 반환한다.
	 * 
	 * @param range84	질의 대상 영역. 위경도(WGS84) 좌표계 사용
	 * @return	질의 결과 클러스터의 식별자 스트림.
	 */
	public FStream<String> queryClusterKeys(Envelope range84);
	
	/**
	 * 주어진 위경도 좌표계 사각 영역과 겹치는 클러스터들을 반환한다.
	 * 
	 * @param range84	질의 영역, 위경도(WGS84) 좌표계 사용
	 * @return	주어진 영역과 겹치는 공간 클러스터 스트림. 
	 */
	public default FStream<T> queryClusters(Envelope range84) {
		return queryClusterKeys(range84).map(qk -> getCluster(qk));
	}
	
	/**
	 * 주어진 위경도 좌표계 사각 영역과 겹치는 레코드들을 반환한다.
	 * 
	 * @param range84	질의 영역, 위경도(WGS84) 좌표계 사용
	 * @return	주어진 영역과 겹치는 공간 레코드 스트림. 
	 */
	public default FStream<Record> query(Envelope range84) {
		return queryClusterKeys(range84)
				.flatMap(qk -> getCluster(qk).query(range84, true))
				.map(EnvelopeTaggedRecord::getRecord);
	}

	/**
	 * 주어진 위경도 좌표계 사각 영역과 겹치는 레코드드의 존재 여부를 반환한다.
	 * 
	 * @param range84	질의 영역, 위경도(WGS84) 좌표계 사용
	 * @return	겹치는 레코드의 존재 여부.
	 */
	public default boolean existsMatch(Envelope range84) {
		return queryClusterKeys(range84)
				.flatMap(qk -> getCluster(qk).query(range84, false))
				.next().isPresent();
	}
	
	/**
	 * 두 클러스터 인덱스에 속한 타일들 중에서 영역이 같거나 포함관계가 있는 클러스터들의
	 * 식별자(quad-key) 쌍의 스트림을 반환한다.
	 * 
	 * @param left 검사대상 전역 인덱스
	 * @param right 검사대상 전역 인덱스
	 * @return	검색된 클러스터 식별자 쌍의 스트림 
	 */
	public static FStream<Match<String>> matchClusterKeys(QuadClusterFile<? extends QuadCluster> left,
														QuadClusterFile<? extends QuadCluster> right) {
		return FStream.from(new QuadClusterMatchIterator<>(left, right));
	}
}
