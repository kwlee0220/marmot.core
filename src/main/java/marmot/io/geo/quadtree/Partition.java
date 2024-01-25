package marmot.io.geo.quadtree;

import org.locationtech.jts.geom.Envelope;

import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface Partition<T extends EnvelopedValue> {
	/**
	 * 파티션에 저장된 value의 갯수를 반환한다.
	 * 
	 * @return	파티션에 저장된 value의 갯수
	 */
	public int size();
	
	/**
	 * 파티션에 저장된 value의 MBR을 반환한다.
	 * 
	 * @return	MBR
	 */
	public Envelope getBounds();
	
	/**
	 * 파티션에 저장된 모든 value를 스트림 형태로 반환한다.
	 * 
	 * @return	value 스트림
	 */
	public FStream<T> values();
	
	/**
	 * 파티션에 저장된 value들 중에서 주어진 query와 겹치는
	 * value들을 스트림 형태로 반환한다.
	 * 
	 * @param query	질의 영역
	 * @return	value 스트림
	 */
	public default FStream<T> intersects(final Envelope query) {
		return values().filter(v -> v.getEnvelope().intersects(query));
	}
	
	/**
	 * 파티션에 새 value를 추가한다.
	 * 파티션 용량이 넘치는 경우 추가가 실패되고, {@code false}가 반환된다.
	 * 
	 * @param value		추가시킬 value
	 * @return	추가여부
	 */
	public boolean add(T value);
	
	/**
	 * 파티션에 새 value를 추가한다.
	 * 파티션 용량이 넘치는 경우 추가가 실패되고, {@code false}가 반환된다.
	 * 
	 * @param value		추가시킬 value
	 * @param reserveForSpeed	insert 성능을 위해 일정량을 파티션에 남길지 여부
	 * @return	추가여부
	 */
	public default boolean add(T value, boolean reserveForSpeed) {
		return add(value);
	}
	
	/**
	 * 파티션에 새 value 추가시 용량부족으로 실패한 경우, 용량 확장을 시도한다.
	 * 용량 확장이 실패한 경우는 {@code false}를 반환한다.
	 * 
	 * @return	용량 확장 여부
	 */
	public default boolean expand() {
		return false;
	}
}