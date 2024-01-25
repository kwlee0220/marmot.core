// Generated from ColumnSelectionExpr.g4 by ANTLR 4.7.2

package marmot.optor.support.colexpr;

import java.util.List;

import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.RuntimeMetaData;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.VocabularyImpl;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ColumnSelectionExprParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, AS=7, ID=8, INT=9, FLOAT=10, 
		STRING=11, LINE_COMMENT=12, COMMENT=13, WS=14;
	public static final int
		RULE_selectionExpr = 0, RULE_columnExpr = 1, RULE_all = 2, RULE_allBut = 3, 
		RULE_fullColNameList = 4, RULE_fullColName = 5, RULE_colNameList = 6, 
		RULE_colName = 7, RULE_namespace = 8, RULE_alias = 9, RULE_idList = 10;
	private static String[] makeRuleNames() {
		return new String[] {
			"selectionExpr", "columnExpr", "all", "allBut", "fullColNameList", "fullColName", 
			"colNameList", "colName", "namespace", "alias", "idList"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "','", "'*'", "'-'", "'{'", "'}'", "'.'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, "AS", "ID", "INT", "FLOAT", 
			"STRING", "LINE_COMMENT", "COMMENT", "WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
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
	public String getGrammarFileName() { return "ColumnSelectionExpr.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public ColumnSelectionExprParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class SelectionExprContext extends ParserRuleContext {
		public List<ColumnExprContext> columnExpr() {
			return getRuleContexts(ColumnExprContext.class);
		}
		public ColumnExprContext columnExpr(int i) {
			return getRuleContext(ColumnExprContext.class,i);
		}
		public SelectionExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectionExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitSelectionExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectionExprContext selectionExpr() throws RecognitionException {
		SelectionExprContext _localctx = new SelectionExprContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_selectionExpr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(22);
			columnExpr();
			setState(27);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(23);
				match(T__0);
				setState(24);
				columnExpr();
				}
				}
				setState(29);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
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

	public static class ColumnExprContext extends ParserRuleContext {
		public FullColNameContext fullColName() {
			return getRuleContext(FullColNameContext.class,0);
		}
		public FullColNameListContext fullColNameList() {
			return getRuleContext(FullColNameListContext.class,0);
		}
		public AllContext all() {
			return getRuleContext(AllContext.class,0);
		}
		public AllButContext allBut() {
			return getRuleContext(AllButContext.class,0);
		}
		public ColumnExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnExpr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitColumnExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnExprContext columnExpr() throws RecognitionException {
		ColumnExprContext _localctx = new ColumnExprContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_columnExpr);
		try {
			setState(34);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(30);
				fullColName();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(31);
				fullColNameList();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(32);
				all();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(33);
				allBut();
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

	public static class AllContext extends ParserRuleContext {
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public AllContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_all; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitAll(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AllContext all() throws RecognitionException {
		AllContext _localctx = new AllContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_all);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(37);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ID) {
				{
				setState(36);
				namespace();
				}
			}

			setState(39);
			match(T__1);
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

	public static class AllButContext extends ParserRuleContext {
		public IdListContext idList() {
			return getRuleContext(IdListContext.class,0);
		}
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public AllButContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_allBut; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitAllBut(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AllButContext allBut() throws RecognitionException {
		AllButContext _localctx = new AllButContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_allBut);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(42);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ID) {
				{
				setState(41);
				namespace();
				}
			}

			setState(44);
			match(T__1);
			setState(45);
			match(T__2);
			setState(46);
			match(T__3);
			setState(47);
			idList();
			setState(48);
			match(T__4);
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

	public static class FullColNameListContext extends ParserRuleContext {
		public ColNameListContext colNameList() {
			return getRuleContext(ColNameListContext.class,0);
		}
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public FullColNameListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fullColNameList; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitFullColNameList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FullColNameListContext fullColNameList() throws RecognitionException {
		FullColNameListContext _localctx = new FullColNameListContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_fullColNameList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(51);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ID) {
				{
				setState(50);
				namespace();
				}
			}

			setState(53);
			match(T__3);
			setState(54);
			colNameList();
			setState(55);
			match(T__4);
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

	public static class FullColNameContext extends ParserRuleContext {
		public ColNameContext colName() {
			return getRuleContext(ColNameContext.class,0);
		}
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public FullColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fullColName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitFullColName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FullColNameContext fullColName() throws RecognitionException {
		FullColNameContext _localctx = new FullColNameContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_fullColName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(58);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				{
				setState(57);
				namespace();
				}
				break;
			}
			setState(60);
			colName();
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

	public static class ColNameListContext extends ParserRuleContext {
		public List<ColNameContext> colName() {
			return getRuleContexts(ColNameContext.class);
		}
		public ColNameContext colName(int i) {
			return getRuleContext(ColNameContext.class,i);
		}
		public ColNameListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colNameList; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitColNameList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColNameListContext colNameList() throws RecognitionException {
		ColNameListContext _localctx = new ColNameListContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_colNameList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(62);
			colName();
			setState(67);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(63);
				match(T__0);
				setState(64);
				colName();
				}
				}
				setState(69);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
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

	public static class ColNameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ColumnSelectionExprParser.ID, 0); }
		public AliasContext alias() {
			return getRuleContext(AliasContext.class,0);
		}
		public ColNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitColName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColNameContext colName() throws RecognitionException {
		ColNameContext _localctx = new ColNameContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_colName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(70);
			match(ID);
			setState(72);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(71);
				alias();
				}
			}

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

	public static class NamespaceContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ColumnSelectionExprParser.ID, 0); }
		public NamespaceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namespace; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitNamespace(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NamespaceContext namespace() throws RecognitionException {
		NamespaceContext _localctx = new NamespaceContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_namespace);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(74);
			match(ID);
			setState(75);
			match(T__5);
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

	public static class AliasContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(ColumnSelectionExprParser.AS, 0); }
		public TerminalNode ID() { return getToken(ColumnSelectionExprParser.ID, 0); }
		public AliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alias; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AliasContext alias() throws RecognitionException {
		AliasContext _localctx = new AliasContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(77);
			match(AS);
			setState(78);
			match(ID);
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

	public static class IdListContext extends ParserRuleContext {
		public List<TerminalNode> ID() { return getTokens(ColumnSelectionExprParser.ID); }
		public TerminalNode ID(int i) {
			return getToken(ColumnSelectionExprParser.ID, i);
		}
		public IdListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_idList; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ColumnSelectionExprVisitor ) return ((ColumnSelectionExprVisitor<? extends T>)visitor).visitIdList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdListContext idList() throws RecognitionException {
		IdListContext _localctx = new IdListContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_idList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(80);
			match(ID);
			setState(85);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__0) {
				{
				{
				setState(81);
				match(T__0);
				setState(82);
				match(ID);
				}
				}
				setState(87);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
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

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\20[\4\2\t\2\4\3\t"+
		"\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4"+
		"\f\t\f\3\2\3\2\3\2\7\2\34\n\2\f\2\16\2\37\13\2\3\3\3\3\3\3\3\3\5\3%\n"+
		"\3\3\4\5\4(\n\4\3\4\3\4\3\5\5\5-\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\5\6\66"+
		"\n\6\3\6\3\6\3\6\3\6\3\7\5\7=\n\7\3\7\3\7\3\b\3\b\3\b\7\bD\n\b\f\b\16"+
		"\bG\13\b\3\t\3\t\5\tK\n\t\3\n\3\n\3\n\3\13\3\13\3\13\3\f\3\f\3\f\7\fV"+
		"\n\f\f\f\16\fY\13\f\3\f\2\2\r\2\4\6\b\n\f\16\20\22\24\26\2\2\2Z\2\30\3"+
		"\2\2\2\4$\3\2\2\2\6\'\3\2\2\2\b,\3\2\2\2\n\65\3\2\2\2\f<\3\2\2\2\16@\3"+
		"\2\2\2\20H\3\2\2\2\22L\3\2\2\2\24O\3\2\2\2\26R\3\2\2\2\30\35\5\4\3\2\31"+
		"\32\7\3\2\2\32\34\5\4\3\2\33\31\3\2\2\2\34\37\3\2\2\2\35\33\3\2\2\2\35"+
		"\36\3\2\2\2\36\3\3\2\2\2\37\35\3\2\2\2 %\5\f\7\2!%\5\n\6\2\"%\5\6\4\2"+
		"#%\5\b\5\2$ \3\2\2\2$!\3\2\2\2$\"\3\2\2\2$#\3\2\2\2%\5\3\2\2\2&(\5\22"+
		"\n\2\'&\3\2\2\2\'(\3\2\2\2()\3\2\2\2)*\7\4\2\2*\7\3\2\2\2+-\5\22\n\2,"+
		"+\3\2\2\2,-\3\2\2\2-.\3\2\2\2./\7\4\2\2/\60\7\5\2\2\60\61\7\6\2\2\61\62"+
		"\5\26\f\2\62\63\7\7\2\2\63\t\3\2\2\2\64\66\5\22\n\2\65\64\3\2\2\2\65\66"+
		"\3\2\2\2\66\67\3\2\2\2\678\7\6\2\289\5\16\b\29:\7\7\2\2:\13\3\2\2\2;="+
		"\5\22\n\2<;\3\2\2\2<=\3\2\2\2=>\3\2\2\2>?\5\20\t\2?\r\3\2\2\2@E\5\20\t"+
		"\2AB\7\3\2\2BD\5\20\t\2CA\3\2\2\2DG\3\2\2\2EC\3\2\2\2EF\3\2\2\2F\17\3"+
		"\2\2\2GE\3\2\2\2HJ\7\n\2\2IK\5\24\13\2JI\3\2\2\2JK\3\2\2\2K\21\3\2\2\2"+
		"LM\7\n\2\2MN\7\b\2\2N\23\3\2\2\2OP\7\t\2\2PQ\7\n\2\2Q\25\3\2\2\2RW\7\n"+
		"\2\2ST\7\3\2\2TV\7\n\2\2US\3\2\2\2VY\3\2\2\2WU\3\2\2\2WX\3\2\2\2X\27\3"+
		"\2\2\2YW\3\2\2\2\13\35$\',\65<EJW";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}