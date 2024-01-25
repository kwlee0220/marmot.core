// Generated from ColumnSelectionExpr.g4 by ANTLR 4.7.2

package marmot.optor.support.colexpr;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link ColumnSelectionExprParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface ColumnSelectionExprVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#selectionExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSelectionExpr(ColumnSelectionExprParser.SelectionExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#columnExpr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColumnExpr(ColumnSelectionExprParser.ColumnExprContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#all}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAll(ColumnSelectionExprParser.AllContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#allBut}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAllBut(ColumnSelectionExprParser.AllButContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#fullColNameList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFullColNameList(ColumnSelectionExprParser.FullColNameListContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#fullColName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFullColName(ColumnSelectionExprParser.FullColNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#colNameList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColNameList(ColumnSelectionExprParser.ColNameListContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#colName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitColName(ColumnSelectionExprParser.ColNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#namespace}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNamespace(ColumnSelectionExprParser.NamespaceContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#alias}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAlias(ColumnSelectionExprParser.AliasContext ctx);
	/**
	 * Visit a parse tree produced by {@link ColumnSelectionExprParser#idList}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdList(ColumnSelectionExprParser.IdListContext ctx);
}