package marmot.optor.support.colexpr;

import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnSelectionException extends MarmotInternalException {
	private static final long serialVersionUID = 8780973921334781152L;
	
	public ColumnSelectionException(String details) {
		super(details);
	}

}
