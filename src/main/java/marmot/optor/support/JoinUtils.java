package marmot.optor.support;

import marmot.Column;
import marmot.RecordSchema;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.optor.support.colexpr.ColumnSelectorFactory;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinUtils {
	private JoinUtils() {
		throw new AssertionError("Should not be called: class=" + JoinUtils.class);
	}
	
	public static ColumnSelector createJoinColumnSelector(RecordSchema left, RecordSchema right,
															FOption<String> outColsExpr) {
		String colsExpr = outColsExpr.getOrElse(() -> getDefaultOutputColumnExpr(left, right));
		return createJoinColumnSelector(left, right, colsExpr);
	}
	
	public static ColumnSelector createJoinColumnSelector(RecordSchema left, RecordSchema right,
															String outColsExpr) {
		ColumnSelectorFactory fact = new ColumnSelectorFactory(outColsExpr);
		fact.addRecordSchema("left", left);
		fact.addRecordSchema("right", right);
		
		return fact.create();
	}
	
	private static String getDefaultOutputColumnExpr(RecordSchema left, RecordSchema right) {
		String leftExpr = listColumNames("left", left).trim();
		String rightExpr = listColumNames("right", right).trim();
		if ( leftExpr.length() > 0 && rightExpr.length() > 0 ) {
			leftExpr = leftExpr + ", ";
		}
		
		return leftExpr + rightExpr;
	}
	
	private static String listColumNames(String prefix, RecordSchema schema) {
		if ( schema.getColumnCount() == 0 ) {
			return "";
		}
		
		if ( prefix != null && prefix.length() > 0) {
			return schema.streamColumns()
						.map(Column::name)
						.map(n -> String.format("%s as %s_%s", n, prefix, n))
						.join(",", String.format("%s.{", prefix), "}");
		}
		else {
			return schema.streamColumns()
						.map(Column::name)
						.join(",");
		}
	}
}
