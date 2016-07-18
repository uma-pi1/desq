// Generated from PatExOld.g4 by ANTLR 4.5

    package patex;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PatExOldParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, WORD=15, CHAR=16, INT=17, 
		WS=18;
	public static final int
		RULE_patex = 0, RULE_unionexp = 1, RULE_concatexp = 2, RULE_repeatexp = 3, 
		RULE_simpleexp = 4, RULE_itemexp = 5;
	public static final String[] ruleNames = {
		"patex", "unionexp", "concatexp", "repeatexp", "simpleexp", "itemexp"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'|'", "'?'", "'*'", "'+'", "'{'", "'}'", "','", "'['", "']'", "'('", 
		"')'", "'.'", "'^'", "'='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, "WORD", "CHAR", "INT", "WS"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "PatExOld.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public PatExOldParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class PatexContext extends ParserRuleContext {
		public PatexContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_patex; }
	 
		public PatexContext() { }
		public void copyFrom(PatexContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class UnionContext extends PatexContext {
		public UnionexpContext unionexp() {
			return getRuleContext(UnionexpContext.class,0);
		}
		public UnionContext(PatexContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterUnion(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitUnion(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitUnion(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PatexContext patex() throws RecognitionException {
		PatexContext _localctx = new PatexContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_patex);
		try {
			_localctx = new UnionContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(12);
			unionexp();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnionexpContext extends ParserRuleContext {
		public UnionexpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unionexp; }
	 
		public UnionexpContext() { }
		public void copyFrom(UnionexpContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ConcatContext extends UnionexpContext {
		public ConcatexpContext concatexp() {
			return getRuleContext(ConcatexpContext.class,0);
		}
		public ConcatContext(UnionexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterConcat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitConcat(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitConcat(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class UnionExpressionContext extends UnionexpContext {
		public ConcatexpContext concatexp() {
			return getRuleContext(ConcatexpContext.class,0);
		}
		public UnionexpContext unionexp() {
			return getRuleContext(UnionexpContext.class,0);
		}
		public UnionExpressionContext(UnionexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterUnionExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitUnionExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitUnionExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnionexpContext unionexp() throws RecognitionException {
		UnionexpContext _localctx = new UnionexpContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_unionexp);
		try {
			setState(19);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				_localctx = new UnionExpressionContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(14);
				concatexp();
				setState(15);
				match(T__0);
				setState(16);
				unionexp();
				}
				break;
			case 2:
				_localctx = new ConcatContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(18);
				concatexp();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConcatexpContext extends ParserRuleContext {
		public ConcatexpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_concatexp; }
	 
		public ConcatexpContext() { }
		public void copyFrom(ConcatexpContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ConcatExpressionContext extends ConcatexpContext {
		public RepeatexpContext repeatexp() {
			return getRuleContext(RepeatexpContext.class,0);
		}
		public ConcatexpContext concatexp() {
			return getRuleContext(ConcatexpContext.class,0);
		}
		public ConcatExpressionContext(ConcatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterConcatExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitConcatExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitConcatExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RepeatExpressionContext extends ConcatexpContext {
		public RepeatexpContext repeatexp() {
			return getRuleContext(RepeatexpContext.class,0);
		}
		public RepeatExpressionContext(ConcatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterRepeatExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitRepeatExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitRepeatExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConcatexpContext concatexp() throws RecognitionException {
		ConcatexpContext _localctx = new ConcatexpContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_concatexp);
		try {
			setState(25);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				_localctx = new ConcatExpressionContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(21);
				repeatexp(0);
				setState(22);
				concatexp();
				}
				break;
			case 2:
				_localctx = new RepeatExpressionContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(24);
				repeatexp(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RepeatexpContext extends ParserRuleContext {
		public RepeatexpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_repeatexp; }
	 
		public RepeatexpContext() { }
		public void copyFrom(RepeatexpContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class OptionalExpressionContext extends RepeatexpContext {
		public RepeatexpContext repeatexp() {
			return getRuleContext(RepeatexpContext.class,0);
		}
		public OptionalExpressionContext(RepeatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterOptionalExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitOptionalExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitOptionalExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RepeatMinMaxExpressionContext extends RepeatexpContext {
		public RepeatexpContext repeatexp() {
			return getRuleContext(RepeatexpContext.class,0);
		}
		public List<TerminalNode> WORD() { return getTokens(PatExOldParser.WORD); }
		public TerminalNode WORD(int i) {
			return getToken(PatExOldParser.WORD, i);
		}
		public RepeatMinMaxExpressionContext(RepeatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterRepeatMinMaxExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitRepeatMinMaxExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitRepeatMinMaxExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RepeatMaxExpressionContext extends RepeatexpContext {
		public RepeatexpContext repeatexp() {
			return getRuleContext(RepeatexpContext.class,0);
		}
		public TerminalNode WORD() { return getToken(PatExOldParser.WORD, 0); }
		public RepeatMaxExpressionContext(RepeatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterRepeatMaxExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitRepeatMaxExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitRepeatMaxExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RepeatMinExpressionContext extends RepeatexpContext {
		public RepeatexpContext repeatexp() {
			return getRuleContext(RepeatexpContext.class,0);
		}
		public TerminalNode WORD() { return getToken(PatExOldParser.WORD, 0); }
		public RepeatMinExpressionContext(RepeatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterRepeatMinExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitRepeatMinExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitRepeatMinExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class SimpleExpressionContext extends RepeatexpContext {
		public SimpleexpContext simpleexp() {
			return getRuleContext(SimpleexpContext.class,0);
		}
		public SimpleExpressionContext(RepeatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterSimpleExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitSimpleExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitSimpleExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class PlusExpressionContext extends RepeatexpContext {
		public RepeatexpContext repeatexp() {
			return getRuleContext(RepeatexpContext.class,0);
		}
		public PlusExpressionContext(RepeatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterPlusExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitPlusExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitPlusExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class StarExpressionContext extends RepeatexpContext {
		public RepeatexpContext repeatexp() {
			return getRuleContext(RepeatexpContext.class,0);
		}
		public StarExpressionContext(RepeatexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterStarExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitStarExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitStarExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RepeatexpContext repeatexp() throws RecognitionException {
		return repeatexp(0);
	}

	private RepeatexpContext repeatexp(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		RepeatexpContext _localctx = new RepeatexpContext(_ctx, _parentState);
		RepeatexpContext _prevctx = _localctx;
		int _startState = 6;
		enterRecursionRule(_localctx, 6, RULE_repeatexp, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			_localctx = new SimpleExpressionContext(_localctx);
			_ctx = _localctx;
			_prevctx = _localctx;

			setState(28);
			simpleexp();
			}
			_ctx.stop = _input.LT(-1);
			setState(53);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(51);
					switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
					case 1:
						{
						_localctx = new OptionalExpressionContext(new RepeatexpContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_repeatexp);
						setState(30);
						if (!(precpred(_ctx, 7))) throw new FailedPredicateException(this, "precpred(_ctx, 7)");
						setState(31);
						match(T__1);
						}
						break;
					case 2:
						{
						_localctx = new StarExpressionContext(new RepeatexpContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_repeatexp);
						setState(32);
						if (!(precpred(_ctx, 6))) throw new FailedPredicateException(this, "precpred(_ctx, 6)");
						setState(33);
						match(T__2);
						}
						break;
					case 3:
						{
						_localctx = new PlusExpressionContext(new RepeatexpContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_repeatexp);
						setState(34);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(35);
						match(T__3);
						}
						break;
					case 4:
						{
						_localctx = new RepeatMaxExpressionContext(new RepeatexpContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_repeatexp);
						setState(36);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(37);
						match(T__4);
						setState(38);
						match(WORD);
						setState(39);
						match(T__5);
						}
						break;
					case 5:
						{
						_localctx = new RepeatMinExpressionContext(new RepeatexpContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_repeatexp);
						setState(40);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(41);
						match(T__4);
						setState(42);
						match(WORD);
						setState(43);
						match(T__6);
						setState(44);
						match(T__5);
						}
						break;
					case 6:
						{
						_localctx = new RepeatMinMaxExpressionContext(new RepeatexpContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_repeatexp);
						setState(45);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(46);
						match(T__4);
						setState(47);
						match(WORD);
						setState(48);
						match(T__6);
						setState(49);
						match(WORD);
						setState(50);
						match(T__5);
						}
						break;
					}
					} 
				}
				setState(55);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class SimpleexpContext extends ParserRuleContext {
		public SimpleexpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_simpleexp; }
	 
		public SimpleexpContext() { }
		public void copyFrom(SimpleexpContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ItemExpressionContext extends SimpleexpContext {
		public ItemexpContext itemexp() {
			return getRuleContext(ItemexpContext.class,0);
		}
		public ItemExpressionContext(SimpleexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterItemExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitItemExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitItemExpression(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class ParensContext extends SimpleexpContext {
		public UnionexpContext unionexp() {
			return getRuleContext(UnionexpContext.class,0);
		}
		public ParensContext(SimpleexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterParens(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitParens(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitParens(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class CaptureContext extends SimpleexpContext {
		public UnionexpContext unionexp() {
			return getRuleContext(UnionexpContext.class,0);
		}
		public CaptureContext(SimpleexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterCapture(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitCapture(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitCapture(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SimpleexpContext simpleexp() throws RecognitionException {
		SimpleexpContext _localctx = new SimpleexpContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_simpleexp);
		try {
			setState(65);
			switch (_input.LA(1)) {
			case T__11:
			case WORD:
				_localctx = new ItemExpressionContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(56);
				itemexp();
				}
				break;
			case T__7:
				_localctx = new ParensContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(57);
				match(T__7);
				setState(58);
				unionexp();
				setState(59);
				match(T__8);
				}
				break;
			case T__9:
				_localctx = new CaptureContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(61);
				match(T__9);
				setState(62);
				unionexp();
				setState(63);
				match(T__10);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ItemexpContext extends ParserRuleContext {
		public ItemexpContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_itemexp; }
	 
		public ItemexpContext() { }
		public void copyFrom(ItemexpContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ItemContext extends ItemexpContext {
		public TerminalNode WORD() { return getToken(PatExOldParser.WORD, 0); }
		public ItemContext(ItemexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitItem(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class WildCardContext extends ItemexpContext {
		public WildCardContext(ItemexpContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).enterWildCard(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof PatExOldListener ) ((PatExOldListener)listener).exitWildCard(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PatExOldVisitor ) return ((PatExOldVisitor<? extends T>)visitor).visitWildCard(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ItemexpContext itemexp() throws RecognitionException {
		ItemexpContext _localctx = new ItemexpContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_itemexp);
		try {
			setState(78);
			switch (_input.LA(1)) {
			case T__11:
				_localctx = new WildCardContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(67);
				match(T__11);
				setState(69);
				switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
				case 1:
					{
					setState(68);
					match(T__12);
					}
					break;
				}
				}
				break;
			case WORD:
				_localctx = new ItemContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(71);
				match(WORD);
				setState(73);
				switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
				case 1:
					{
					setState(72);
					match(T__13);
					}
					break;
				}
				setState(76);
				switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
				case 1:
					{
					setState(75);
					match(T__12);
					}
					break;
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 3:
			return repeatexp_sempred((RepeatexpContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean repeatexp_sempred(RepeatexpContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 7);
		case 1:
			return precpred(_ctx, 6);
		case 2:
			return precpred(_ctx, 5);
		case 3:
			return precpred(_ctx, 4);
		case 4:
			return precpred(_ctx, 3);
		case 5:
			return precpred(_ctx, 2);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\24S\4\2\t\2\4\3\t"+
		"\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\3\2\3\2\3\3\3\3\3\3\3\3\3\3\5\3\26"+
		"\n\3\3\4\3\4\3\4\3\4\5\4\34\n\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3"+
		"\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\7\5\66\n\5"+
		"\f\5\16\59\13\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6D\n\6\3\7\3\7\5"+
		"\7H\n\7\3\7\3\7\5\7L\n\7\3\7\5\7O\n\7\5\7Q\n\7\3\7\2\3\b\b\2\4\6\b\n\f"+
		"\2\2Z\2\16\3\2\2\2\4\25\3\2\2\2\6\33\3\2\2\2\b\35\3\2\2\2\nC\3\2\2\2\f"+
		"P\3\2\2\2\16\17\5\4\3\2\17\3\3\2\2\2\20\21\5\6\4\2\21\22\7\3\2\2\22\23"+
		"\5\4\3\2\23\26\3\2\2\2\24\26\5\6\4\2\25\20\3\2\2\2\25\24\3\2\2\2\26\5"+
		"\3\2\2\2\27\30\5\b\5\2\30\31\5\6\4\2\31\34\3\2\2\2\32\34\5\b\5\2\33\27"+
		"\3\2\2\2\33\32\3\2\2\2\34\7\3\2\2\2\35\36\b\5\1\2\36\37\5\n\6\2\37\67"+
		"\3\2\2\2 !\f\t\2\2!\66\7\4\2\2\"#\f\b\2\2#\66\7\5\2\2$%\f\7\2\2%\66\7"+
		"\6\2\2&\'\f\6\2\2\'(\7\7\2\2()\7\21\2\2)\66\7\b\2\2*+\f\5\2\2+,\7\7\2"+
		"\2,-\7\21\2\2-.\7\t\2\2.\66\7\b\2\2/\60\f\4\2\2\60\61\7\7\2\2\61\62\7"+
		"\21\2\2\62\63\7\t\2\2\63\64\7\21\2\2\64\66\7\b\2\2\65 \3\2\2\2\65\"\3"+
		"\2\2\2\65$\3\2\2\2\65&\3\2\2\2\65*\3\2\2\2\65/\3\2\2\2\669\3\2\2\2\67"+
		"\65\3\2\2\2\678\3\2\2\28\t\3\2\2\29\67\3\2\2\2:D\5\f\7\2;<\7\n\2\2<=\5"+
		"\4\3\2=>\7\13\2\2>D\3\2\2\2?@\7\f\2\2@A\5\4\3\2AB\7\r\2\2BD\3\2\2\2C:"+
		"\3\2\2\2C;\3\2\2\2C?\3\2\2\2D\13\3\2\2\2EG\7\16\2\2FH\7\17\2\2GF\3\2\2"+
		"\2GH\3\2\2\2HQ\3\2\2\2IK\7\21\2\2JL\7\20\2\2KJ\3\2\2\2KL\3\2\2\2LN\3\2"+
		"\2\2MO\7\17\2\2NM\3\2\2\2NO\3\2\2\2OQ\3\2\2\2PE\3\2\2\2PI\3\2\2\2Q\r\3"+
		"\2\2\2\13\25\33\65\67CGKNP";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}