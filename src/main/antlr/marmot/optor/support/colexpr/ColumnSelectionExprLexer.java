// Generated from ColumnSelectionExpr.g4 by ANTLR 4.7.2

package marmot.optor.support.colexpr;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RuntimeMetaData;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.VocabularyImpl;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ColumnSelectionExprLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.7.2", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, AS=7, ID=8, INT=9, FLOAT=10, 
		STRING=11, LINE_COMMENT=12, COMMENT=13, WS=14;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "AS", "ID", "INT", "FLOAT", 
			"ID_LETTER", "DIGIT", "STRING", "ESC", "LINE_COMMENT", "COMMENT", "WS"
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


	public ColumnSelectionExprLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "ColumnSelectionExpr.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\20\u008b\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\b\3\b"+
		"\5\b\66\n\b\3\t\3\t\3\t\7\t;\n\t\f\t\16\t>\13\t\3\n\6\nA\n\n\r\n\16\n"+
		"B\3\13\6\13F\n\13\r\13\16\13G\3\13\3\13\7\13L\n\13\f\13\16\13O\13\13\3"+
		"\13\3\13\6\13S\n\13\r\13\16\13T\5\13W\n\13\3\f\3\f\3\r\3\r\3\16\3\16\3"+
		"\16\7\16`\n\16\f\16\16\16c\13\16\3\16\3\16\3\17\3\17\3\17\3\20\3\20\3"+
		"\20\3\20\7\20n\n\20\f\20\16\20q\13\20\3\20\3\20\3\20\3\20\3\21\3\21\3"+
		"\21\3\21\7\21{\n\21\f\21\16\21~\13\21\3\21\3\21\3\21\3\21\3\21\3\22\6"+
		"\22\u0086\n\22\r\22\16\22\u0087\3\22\3\22\5ao|\2\23\3\3\5\4\7\5\t\6\13"+
		"\7\r\b\17\t\21\n\23\13\25\f\27\2\31\2\33\r\35\2\37\16!\17#\20\3\2\5\b"+
		"\2&&\61\61C\\aac|\u0082\0\b\2$$^^ddppttvv\5\2\13\f\17\17\"\"\2\u0094\2"+
		"\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2"+
		"\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\33\3\2\2\2\2"+
		"\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\3%\3\2\2\2\5\'\3\2\2\2\7)\3\2\2\2\t"+
		"+\3\2\2\2\13-\3\2\2\2\r/\3\2\2\2\17\65\3\2\2\2\21\67\3\2\2\2\23@\3\2\2"+
		"\2\25V\3\2\2\2\27X\3\2\2\2\31Z\3\2\2\2\33\\\3\2\2\2\35f\3\2\2\2\37i\3"+
		"\2\2\2!v\3\2\2\2#\u0085\3\2\2\2%&\7.\2\2&\4\3\2\2\2\'(\7,\2\2(\6\3\2\2"+
		"\2)*\7/\2\2*\b\3\2\2\2+,\7}\2\2,\n\3\2\2\2-.\7\177\2\2.\f\3\2\2\2/\60"+
		"\7\60\2\2\60\16\3\2\2\2\61\62\7C\2\2\62\66\7U\2\2\63\64\7c\2\2\64\66\7"+
		"u\2\2\65\61\3\2\2\2\65\63\3\2\2\2\66\20\3\2\2\2\67<\5\27\f\28;\5\27\f"+
		"\29;\5\31\r\2:8\3\2\2\2:9\3\2\2\2;>\3\2\2\2<:\3\2\2\2<=\3\2\2\2=\22\3"+
		"\2\2\2><\3\2\2\2?A\5\31\r\2@?\3\2\2\2AB\3\2\2\2B@\3\2\2\2BC\3\2\2\2C\24"+
		"\3\2\2\2DF\5\31\r\2ED\3\2\2\2FG\3\2\2\2GE\3\2\2\2GH\3\2\2\2HI\3\2\2\2"+
		"IM\7\60\2\2JL\5\31\r\2KJ\3\2\2\2LO\3\2\2\2MK\3\2\2\2MN\3\2\2\2NW\3\2\2"+
		"\2OM\3\2\2\2PR\7\60\2\2QS\5\31\r\2RQ\3\2\2\2ST\3\2\2\2TR\3\2\2\2TU\3\2"+
		"\2\2UW\3\2\2\2VE\3\2\2\2VP\3\2\2\2W\26\3\2\2\2XY\t\2\2\2Y\30\3\2\2\2Z"+
		"[\4\62;\2[\32\3\2\2\2\\a\7)\2\2]`\5\35\17\2^`\13\2\2\2_]\3\2\2\2_^\3\2"+
		"\2\2`c\3\2\2\2ab\3\2\2\2a_\3\2\2\2bd\3\2\2\2ca\3\2\2\2de\7)\2\2e\34\3"+
		"\2\2\2fg\7^\2\2gh\t\3\2\2h\36\3\2\2\2ij\7\61\2\2jk\7\61\2\2ko\3\2\2\2"+
		"ln\13\2\2\2ml\3\2\2\2nq\3\2\2\2op\3\2\2\2om\3\2\2\2pr\3\2\2\2qo\3\2\2"+
		"\2rs\7\f\2\2st\3\2\2\2tu\b\20\2\2u \3\2\2\2vw\7\61\2\2wx\7,\2\2x|\3\2"+
		"\2\2y{\13\2\2\2zy\3\2\2\2{~\3\2\2\2|}\3\2\2\2|z\3\2\2\2}\177\3\2\2\2~"+
		"|\3\2\2\2\177\u0080\7,\2\2\u0080\u0081\7\61\2\2\u0081\u0082\3\2\2\2\u0082"+
		"\u0083\b\21\2\2\u0083\"\3\2\2\2\u0084\u0086\t\4\2\2\u0085\u0084\3\2\2"+
		"\2\u0086\u0087\3\2\2\2\u0087\u0085\3\2\2\2\u0087\u0088\3\2\2\2\u0088\u0089"+
		"\3\2\2\2\u0089\u008a\b\22\2\2\u008a$\3\2\2\2\20\2\65:<BGMTV_ao|\u0087"+
		"\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}