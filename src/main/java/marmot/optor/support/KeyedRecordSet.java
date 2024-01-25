package marmot.optor.support;

import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.RecordKey;

/**
 * {@code GroupedRecordSet}는 레코드에 포함된 일부 컬럼을 기준으로 그룹핑된
 * 레코드 세트의 인터페이스를 정의한다.
 * <p>
 * 같은 {@code GroupedRecordSet}에 포함된 레코드들은 모두 동일 그룹 키 값을 갖는다.
 * 그룹 키 값은 {@link #getKey()}를 통해 얻을 수 있고,
 * 그룹 키를 구성하는 컬럼들은 {@link #getKeySchema()}를 통해 얻을 수 있다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface KeyedRecordSet extends RecordSet {
	/**
	 * 본 {@code GroupedRecordSet}의 그룹 키 컬럼 값 리스트를 반환한다.
	 * 
	 * @return	그룹 키 컬럼 값 리스트.
	 */
	public RecordKey getKey();
	
	public RecordSchema getKeySchema();
	
	public default RecordSet flatten() {
		return new FlattenKeyedRecordSet(this);
	}
}