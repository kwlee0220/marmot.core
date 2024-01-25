package marmot.support;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PNU {
	public static final int SIDO_SEOUL = 11;
	public static final int SIDO_PUSAN = 26;
	public static final int SIDO_DAEGU = 27;
	public static final int SIDO_INCHEN = 28;
	public static final int SIDO_GWANGJU = 29;
	public static final int SIDO_DAEJEON = 30;
	public static final int SIDO_UNSAN = 31;
	public static final int SIDO_SEJONG = 36;
	public static final int SIDO_KYOUNGGI = 41;
	public static final int SIDO_KANGWON = 42;
	public static final int SIDO_CHUNGBUK = 43;
	public static final int SIDO_CHUNGNAM = 44;
	public static final int SIDO_CHUNBUK = 45;
	public static final int SIDO_CHUNNAM = 46;
	public static final int SIDO_KYOUNGBUK = 47;
	public static final int SIDO_KYOUNGNAM = 48;
	public static final int SIDO_JEJU = 50;
	
	private String m_code;
	
	public PNU(String code) {
		m_code = code;
	}
	
	/**
	 * 시도 번호(2자리)를 반환한다.
	 * 
	 * @return	시도 번호
	 */
	public int getSido() {
		return Integer.parseInt(m_code.substring(0, 2));
	}

	/**
	 * 시군구 번호(3자리)를 반환한다.
	 * 
	 * @return	시군구 번호
	 */
	public int getSiGunGo() {
		return Integer.parseInt(m_code.substring(2, 5));
	}
	
	/**
	 * 읍면동 번호(3자리)를 반환한다.
	 * 
	 * @return	읍면동 번호
	 */
	public String getEupMyunDong() {
		return m_code.substring(5, 8);
	}

	/**
	 * 리 번호(2자리)를 반환한다.
	 * 
	 * @return	리 번호
	 */
	public String getRi() {
		return m_code.substring(8, 10);
	}

	/**
	 * 토지구분 번호(1자리)를 반환한다.
	 * 
	 * @return	토지구분 번호
	 */
	public String getPilJi() {
		return m_code.substring(10, 11);
	}

	/**
	 * 본번 번호(4자리)를 반환한다.
	 * 
	 * @return	본번 번호
	 */
	public String getBonBun() {
		return m_code.substring(11, 15);
	}

	/**
	 * 부번 번호(4자리)를 반환한다.
	 * 
	 * @return	부번 번호
	 */
	public String getBooBun() {
		return m_code.substring(15, 19);
	}
	
//	토지코드(PNU)는 19자리의 숫자로 구성되어 있으며 각각의 의미는 다음과 같습니다.
//	 - 시도(2)+시군구(3)+읍면동(3)+리(2)+토지구분1(1)+본번(4)+부번(4)
//	 - 2720010200106040023(대구광역시 남구 봉덕동 604-23 번지)
}
