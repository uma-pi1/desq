// Generated from PatExOld.g4 by ANTLR 4.5

    package patex;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PatExOldParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PatExOldVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by the {@code union}
	 * labeled alternative in {@link PatExOldParser#patex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnion(PatExOldParser.UnionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code unionExpression}
	 * labeled alternative in {@link PatExOldParser#unionexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnionExpression(PatExOldParser.UnionExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code concat}
	 * labeled alternative in {@link PatExOldParser#unionexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcat(PatExOldParser.ConcatContext ctx);
	/**
	 * Visit a parse tree produced by the {@code concatExpression}
	 * labeled alternative in {@link PatExOldParser#concatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConcatExpression(PatExOldParser.ConcatExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatExpression}
	 * labeled alternative in {@link PatExOldParser#concatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatExpression(PatExOldParser.RepeatExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code optionalExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitOptionalExpression(PatExOldParser.OptionalExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatMinMaxExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatMinMaxExpression(PatExOldParser.RepeatMinMaxExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatMaxExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatMaxExpression(PatExOldParser.RepeatMaxExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code repeatMinExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRepeatMinExpression(PatExOldParser.RepeatMinExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code simpleExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSimpleExpression(PatExOldParser.SimpleExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code plusExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPlusExpression(PatExOldParser.PlusExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code starExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStarExpression(PatExOldParser.StarExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code itemExpression}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitItemExpression(PatExOldParser.ItemExpressionContext ctx);
	/**
	 * Visit a parse tree produced by the {@code parens}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParens(PatExOldParser.ParensContext ctx);
	/**
	 * Visit a parse tree produced by the {@code capture}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCapture(PatExOldParser.CaptureContext ctx);
	/**
	 * Visit a parse tree produced by the {@code wildCard}
	 * labeled alternative in {@link PatExOldParser#itemexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildCard(PatExOldParser.WildCardContext ctx);
	/**
	 * Visit a parse tree produced by the {@code item}
	 * labeled alternative in {@link PatExOldParser#itemexp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitItem(PatExOldParser.ItemContext ctx);
}