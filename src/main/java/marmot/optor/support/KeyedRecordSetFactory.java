package marmot.optor.support;

import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.support.ProgressReportable;
import utils.func.FOption;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface KeyedRecordSetFactory extends AutoCloseable, ProgressReportable {
	/**
	 * Groupping에 사용할 컬럼 이름들을 반환한다.
	 * 
	 * @return	컬럼 이름들
	 */
	public MultiColumnKey getKeyColumns();
	
	/**
	 * Groupping에 사용할 컬럼들로 구성된 레코드 스키마를 반환한다.
	 * 
	 * @return 레코드 스키마.
	 */
	public RecordSchema getKeySchema();
	
	/**
	 * 생성되는 레코드 세트의 스키마를 반환한다.
	 * 
	 * @return	레코드 세트의 스키마
	 */
	public RecordSchema getRecordSchema();
	
	/**
	 * 다음 레코드 세트를 반환한다.
	 * 
	 * @return	레코드 세트. 레코드 세트가 존재하지 않는 경우는
	 * 			{@link FOption#empty()}이 반환된다.
	 */
	public FOption<KeyedRecordSet> nextKeyedRecordSet();
	
	public default Try<Void> closeQuietly() {
		return Try.run(this::close);
	}
}
