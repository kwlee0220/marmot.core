package marmot.optor.support.colexpr;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import marmot.RecordSchema;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.AliasContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.AllButContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.AllContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.ColNameContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.ColNameListContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.ColumnExprContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.FullColNameContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.FullColNameListContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.IdListContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.NamespaceContext;
import marmot.optor.support.colexpr.ColumnSelectionExprParser.SelectionExprContext;

import utils.Tuple;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnSelectorFactory {
	public static final String XML_NAME = "column_selector_factory";
	
	private Map<String,RecordSchema> m_schemas = Maps.newHashMap();
	private final String m_colExpr;
	private Set<SelectedColumnInfo> m_selectedsCached;
	private RecordSchema m_schemaCached;
	
	public static ColumnSelector create(RecordSchema schema, String columnExpr) {
		ColumnSelectorFactory fact = new ColumnSelectorFactory(columnExpr);
		fact.addRecordSchema(schema);
		return fact.create();
	}
	
	public ColumnSelectorFactory(String columnExpression) {
		Utilities.checkNotNullArgument(columnExpression, "column expression is null");
		
		m_colExpr = columnExpression;
	}
	
	public RecordSchema getSourceRecordSchema(String alias) {
		Utilities.checkNotNullArgument(alias, "RecordSchema alias is null");
		
		return m_schemas.get(alias);
	}
	
	public void addRecordSchema(RecordSchema schema) throws ColumnSelectionException {
		addRecordSchema("", schema);
	}
	
	public void addRecordSchema(String alias, RecordSchema schema) throws ColumnSelectionException {
		Utilities.checkNotNullArgument(alias, "RecordSchema alias is null");
		Utilities.checkNotNullArgument(schema, "RecordSchema is null");
		
		if ( m_schemas.putIfAbsent(alias, schema) != null ) {
			throw new ColumnSelectionException("alias already exists: alias=" + alias);
		}
		m_selectedsCached = null;
		m_schemaCached = null;
	}
	
	public boolean addOrReplaceRecordSchema(String alias, RecordSchema schema) {
		Utilities.checkNotNullArgument(alias, "RecordSchema alias is null");
		Utilities.checkNotNullArgument(schema, "RecordSchema is null");
		
		boolean added = ( m_schemas.put(alias, schema) == null );
		m_selectedsCached = null;
		m_schemaCached = null;
		
		return added;
	}
	
	public String getColumnExpression() {
		return m_colExpr;
	}
	
	public RecordSchema getRecordSchema() throws ColumnSelectionException {
		if ( m_schemaCached != null ) {
			return m_schemaCached;
		}
		
		if ( m_schemas.size() == 0 ) {
			throw new ColumnSelectionException("input RecordSchema has not been set");
		}
		
		Set<SelectedColumnInfo> selecteds = (m_selectedsCached != null)
											? m_selectedsCached
											: (m_selectedsCached = parseColumnExpression(m_colExpr));
		
		RecordSchema.Builder builder = RecordSchema.builder();
		selecteds.stream()
				.forEach(info -> builder.addColumn(info.m_alias, info.m_column.type()));
		return m_schemaCached = builder.build();
	}
	
	public ColumnSelector create() throws ColumnSelectionException {
		RecordSchema schema = getRecordSchema();
		return new ColumnSelector(m_colExpr, m_selectedsCached, schema);
	}
	
	@Override
	public String toString() {
		return m_colExpr;
	}
	
	private Set<SelectedColumnInfo> parseColumnExpression(String colExprString) {
		colExprString = colExprString.trim();
		if ( colExprString.length() == 0 ) {
			return Sets.newHashSet();
		}
		
		ColumnSelectionExprLexer lexer = new ColumnSelectionExprLexer(new ANTLRInputStream(colExprString));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		ColumnSelectionExprParser parser = new ColumnSelectionExprParser(tokens);
		
		ParseTree tree = parser.selectionExpr();
		Visitor visitor = new Visitor();
		visitor.visit(tree);
		
		return visitor.getSelectedColumnInfos();
	}
	
	class Visitor extends ColumnSelectionExprBaseVisitor<Object> {
		private Set<SelectedColumnInfo> m_selecteds = Sets.newLinkedHashSet();
		
		Set<SelectedColumnInfo> getSelectedColumnInfos() {
			return m_selecteds;
		}
		
		@Override
		public Object visitSelectionExpr(SelectionExprContext ctx) throws ColumnSelectionException {
			for ( ColumnExprContext exprCtx: ctx.columnExpr() ) {
				visitColumnExpr(exprCtx);
			}
			return null;
		}
		
		@Override
		public Object visitColumnExpr(ColumnExprContext ctx) throws ColumnSelectionException {
			ParseTree child = ctx.getChild(0);
			if ( child instanceof FullColNameContext ) {
				visitFullColName((FullColNameContext)child);
			}
			else if ( child instanceof FullColNameListContext ) {
				visitFullColNameList((FullColNameListContext)child);
			}
			else if ( child instanceof AllContext ) {
				visitAll((AllContext)child);
			}
			else if ( child instanceof AllButContext ) {
				visitAllBut((AllButContext)child);
			}
			return null;
		}
		
		@Override
		public Object visitAll(AllContext ctx) throws ColumnSelectionException {
			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
			
			RecordSchema schema = m_schemas.get(ns);
			if ( schema == null ) {
				String details = String.format("unknown namespace: namespace='%s'", ns);
				throw new ColumnSelectionException(details);
			}
			
			schema.getColumns().stream()
					.forEach(col -> m_selecteds.add(new SelectedColumnInfo(ns, col)));
			
			return null;
		}
		
		@Override
		public Object visitAllBut(AllButContext ctx) throws ColumnSelectionException {
			IdListContext idListCtx = ctx.getChild(IdListContext.class, 0);
			List<String> colNameList = visitIdList(idListCtx);
			
			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
			RecordSchema schema = m_schemas.get(ns);
			if ( schema == null ) {
				String details = String.format("unknown namespace: namespace='%s'", ns);
				throw new ColumnSelectionException(details);
			}
			
			Set<String> keys = FStream.from(colNameList).map(String::toLowerCase).toSet(); 
			schema.streamColumns()
					.filter(c -> !keys.contains(c.name().toLowerCase()))
					.map(col -> new SelectedColumnInfo(ns, col))
					.forEach(m_selecteds::add);
			
			return null;
		}
		
		@Override
		public Object visitFullColNameList(FullColNameListContext ctx) {
			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
			
			ColNameListContext colNameListCtx = ctx.getChild(ColNameListContext.class, 0);
			visitColNameList(colNameListCtx).stream()
					.forEach(t -> handleLiteral(ns, t._1, t._2));
			return null;
		}
		
		@Override
		public Object visitFullColName(FullColNameContext ctx) throws ColumnSelectionException {
			NamespaceContext nsCtx = ctx.getChild(NamespaceContext.class, 0);
			String ns = (nsCtx != null) ? nsCtx.getChild(0).getText() : "";
			
			ColNameContext colNameCtx = ctx.getChild(ColNameContext.class, 0);
			Tuple<String,String> colName = visitColName(colNameCtx);
			
			handleLiteral(ns, colName._1, colName._2);
			return null;
		}
		
		@Override
		public List<Tuple<String,String>> visitColNameList(ColNameListContext ctx) {
			return ctx.children.stream()
						.filter(x -> x instanceof ColNameContext)
						.map(x -> visitColName((ColNameContext)x))
						.collect(Collectors.toList());
		}
		
		@Override
		public Tuple<String,String> visitColName(ColNameContext ctx) {
			String colName = ctx.getChild(0).getText();
			AliasContext aliasCtx = ctx.getChild(AliasContext.class, 0);
			if ( aliasCtx != null ) {
				String alias = aliasCtx.getChild(1).getText();
				return Tuple.of(colName, alias);
			}
			else {
				return Tuple.of(colName, null);
			}
		}
		
		@Override
		public List<String> visitIdList(IdListContext ctx) {
			return FStream.from(ctx.children)
							.map(ParseTree::getText)
							.filter(text -> !text.equals(","))
							.toList();
		}
		
		private void handleLiteral(String ns, String colName, String alias)
			throws ColumnSelectionException {
			RecordSchema schema = m_schemas.get(ns);
			if ( schema == null ) {
				String details = String.format("unknown namespace: namespace='%s'", ns);
				throw new ColumnSelectionException(details);
			}
			
			SelectedColumnInfo info = schema.findColumn(colName)
											.map(col -> new SelectedColumnInfo(ns, col))
											.getOrThrow(() -> {
												String details = String.format("unknown column: [%s:%s], schema=%s", ns, colName, schema);
												throw new ColumnSelectionException(details);
											});
			if ( alias != null ) {
				info.setAlias(alias);
			}
			m_selecteds.add(info);
		}
	}
}
