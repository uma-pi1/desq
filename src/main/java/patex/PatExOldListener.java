// Generated from PatExOld.g4 by ANTLR 4.5

    package patex;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link PatExOldParser}.
 */
public interface PatExOldListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by the {@code union}
	 * labeled alternative in {@link PatExOldParser#patex}.
	 * @param ctx the parse tree
	 */
	void enterUnion(PatExOldParser.UnionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code union}
	 * labeled alternative in {@link PatExOldParser#patex}.
	 * @param ctx the parse tree
	 */
	void exitUnion(PatExOldParser.UnionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code unionExpression}
	 * labeled alternative in {@link PatExOldParser#unionexp}.
	 * @param ctx the parse tree
	 */
	void enterUnionExpression(PatExOldParser.UnionExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code unionExpression}
	 * labeled alternative in {@link PatExOldParser#unionexp}.
	 * @param ctx the parse tree
	 */
	void exitUnionExpression(PatExOldParser.UnionExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code concat}
	 * labeled alternative in {@link PatExOldParser#unionexp}.
	 * @param ctx the parse tree
	 */
	void enterConcat(PatExOldParser.ConcatContext ctx);
	/**
	 * Exit a parse tree produced by the {@code concat}
	 * labeled alternative in {@link PatExOldParser#unionexp}.
	 * @param ctx the parse tree
	 */
	void exitConcat(PatExOldParser.ConcatContext ctx);
	/**
	 * Enter a parse tree produced by the {@code concatExpression}
	 * labeled alternative in {@link PatExOldParser#concatexp}.
	 * @param ctx the parse tree
	 */
	void enterConcatExpression(PatExOldParser.ConcatExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code concatExpression}
	 * labeled alternative in {@link PatExOldParser#concatexp}.
	 * @param ctx the parse tree
	 */
	void exitConcatExpression(PatExOldParser.ConcatExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatExpression}
	 * labeled alternative in {@link PatExOldParser#concatexp}.
	 * @param ctx the parse tree
	 */
	void enterRepeatExpression(PatExOldParser.RepeatExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatExpression}
	 * labeled alternative in {@link PatExOldParser#concatexp}.
	 * @param ctx the parse tree
	 */
	void exitRepeatExpression(PatExOldParser.RepeatExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code optionalExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterOptionalExpression(PatExOldParser.OptionalExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code optionalExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitOptionalExpression(PatExOldParser.OptionalExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatMinMaxExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterRepeatMinMaxExpression(PatExOldParser.RepeatMinMaxExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatMinMaxExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitRepeatMinMaxExpression(PatExOldParser.RepeatMinMaxExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatMaxExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterRepeatMaxExpression(PatExOldParser.RepeatMaxExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatMaxExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitRepeatMaxExpression(PatExOldParser.RepeatMaxExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code repeatMinExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterRepeatMinExpression(PatExOldParser.RepeatMinExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code repeatMinExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitRepeatMinExpression(PatExOldParser.RepeatMinExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code simpleExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterSimpleExpression(PatExOldParser.SimpleExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code simpleExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitSimpleExpression(PatExOldParser.SimpleExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code plusExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterPlusExpression(PatExOldParser.PlusExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code plusExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitPlusExpression(PatExOldParser.PlusExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code starExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void enterStarExpression(PatExOldParser.StarExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code starExpression}
	 * labeled alternative in {@link PatExOldParser#repeatexp}.
	 * @param ctx the parse tree
	 */
	void exitStarExpression(PatExOldParser.StarExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code itemExpression}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void enterItemExpression(PatExOldParser.ItemExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code itemExpression}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void exitItemExpression(PatExOldParser.ItemExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parens}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void enterParens(PatExOldParser.ParensContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parens}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void exitParens(PatExOldParser.ParensContext ctx);
	/**
	 * Enter a parse tree produced by the {@code capture}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void enterCapture(PatExOldParser.CaptureContext ctx);
	/**
	 * Exit a parse tree produced by the {@code capture}
	 * labeled alternative in {@link PatExOldParser#simpleexp}.
	 * @param ctx the parse tree
	 */
	void exitCapture(PatExOldParser.CaptureContext ctx);
	/**
	 * Enter a parse tree produced by the {@code wildCard}
	 * labeled alternative in {@link PatExOldParser#itemexp}.
	 * @param ctx the parse tree
	 */
	void enterWildCard(PatExOldParser.WildCardContext ctx);
	/**
	 * Exit a parse tree produced by the {@code wildCard}
	 * labeled alternative in {@link PatExOldParser#itemexp}.
	 * @param ctx the parse tree
	 */
	void exitWildCard(PatExOldParser.WildCardContext ctx);
	/**
	 * Enter a parse tree produced by the {@code item}
	 * labeled alternative in {@link PatExOldParser#itemexp}.
	 * @param ctx the parse tree
	 */
	void enterItem(PatExOldParser.ItemContext ctx);
	/**
	 * Exit a parse tree produced by the {@code item}
	 * labeled alternative in {@link PatExOldParser#itemexp}.
	 * @param ctx the parse tree
	 */
	void exitItem(PatExOldParser.ItemContext ctx);
}