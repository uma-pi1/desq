// Generated from PatEx.g4 by ANTLR 4.5

    package patex;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PatExParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PatExVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by the {@code union}
	 * labeled alternative in {@link PatExParser#patex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnion(PatExParser.UnionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unionExpression}
	 * labeled alternative in {@link PatExParser#unionexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnionExpression(PatExParser.UnionExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code concat}
	 * labeled alternative in {@link PatExParser#unionexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcat(PatExParser.ConcatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code concatExpression}
	 * labeled alternative in {@link PatExParser#concatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcatExpression(PatExParser.ConcatExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatExpression}
	 * labeled alternative in {@link PatExParser#concatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatExpression(PatExParser.RepeatExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code optionalExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOptionalExpression(PatExParser.OptionalExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatMinMaxExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatMinMaxExpression(PatExParser.RepeatMinMaxExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatMaxExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatMaxExpression(PatExParser.RepeatMaxExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatMinExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatMinExpression(PatExParser.RepeatMinExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleExpression(PatExParser.SimpleExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code plusExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPlusExpression(PatExParser.PlusExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code starExpression}
	 * labeled alternative in {@link PatExParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStarExpression(PatExParser.StarExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code itemExpression}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitItemExpression(PatExParser.ItemExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parens}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParens(PatExParser.ParensContext ctx);
	/**
	 * Visit a parse tree produced by the {@code capture}
	 * labeled alternative in {@link PatExParser#simpleexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCapture(PatExParser.CaptureContext ctx);
	/**
	 * Visit a parse tree produced by the {@code wildCard}
	 * labeled alternative in {@link PatExParser#itemexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildCard(PatExParser.WildCardContext ctx);
	/**
	 * Visit a parse tree produced by the {@code item}
	 * labeled alternative in {@link PatExParser#itemexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitItem(PatExParser.ItemContext ctx);
}