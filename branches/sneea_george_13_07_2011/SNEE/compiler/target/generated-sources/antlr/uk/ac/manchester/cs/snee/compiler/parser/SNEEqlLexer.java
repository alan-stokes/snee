// $ANTLR 2.7.4: "SNEEql.g" -> "SNEEqlLexer.java"$

//updated grammar which includes CREATE statements for Data Analysis Techniques
package uk.ac.manchester.cs.snee.compiler.parser;

import java.io.InputStream;
import antlr.TokenStreamException;
import antlr.TokenStreamIOException;
import antlr.TokenStreamRecognitionException;
import antlr.CharStreamException;
import antlr.CharStreamIOException;
import antlr.ANTLRException;
import java.io.Reader;
import java.util.Hashtable;
import antlr.CharScanner;
import antlr.InputBuffer;
import antlr.ByteBuffer;
import antlr.CharBuffer;
import antlr.Token;
import antlr.CommonToken;
import antlr.RecognitionException;
import antlr.NoViableAltForCharException;
import antlr.MismatchedCharException;
import antlr.TokenStream;
import antlr.ANTLRHashString;
import antlr.LexerSharedInputState;
import antlr.collections.impl.BitSet;
import antlr.SemanticException;

public class SNEEqlLexer extends antlr.CharScanner implements SNEEqlParserTokenTypes, TokenStream
 {
public SNEEqlLexer(InputStream in) {
	this(new ByteBuffer(in));
}
public SNEEqlLexer(Reader in) {
	this(new CharBuffer(in));
}
public SNEEqlLexer(InputBuffer ib) {
	this(new LexerSharedInputState(ib));
}
public SNEEqlLexer(LexerSharedInputState state) {
	super(state);
	caseSensitiveLiterals = false;
	setCaseSensitive(false);
	literals = new Hashtable();
	literals.put(new ANTLRHashString("AT", this), new Integer(35));
	literals.put(new ANTLRHashString("classifier", this), new Integer(42));
	literals.put(new ANTLRHashString("to", this), new Integer(34));
	literals.put(new ANTLRHashString("probfn", this), new Integer(52));
	literals.put(new ANTLRHashString("rescan", this), new Integer(33));
	literals.put(new ANTLRHashString("random", this), new Integer(51));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_LOCAL_NAME", this), new Integer(63));
	literals.put(new ANTLRHashString("view", this), new Integer(54));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_UNIT_NAME", this), new Integer(59));
	literals.put(new ANTLRHashString("AS", this), new Integer(16));
	literals.put(new ANTLRHashString("create", this), new Integer(41));
	literals.put(new ANTLRHashString("select", this), new Integer(9));
	literals.put(new ANTLRHashString("d3", this), new Integer(49));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_SOURCE", this), new Integer(60));
	literals.put(new ANTLRHashString("unbounded", this), new Integer(32));
	literals.put(new ANTLRHashString("apriori", this), new Integer(47));
	literals.put(new ANTLRHashString("where", this), new Integer(13));
	literals.put(new ANTLRHashString("dstream", this), new Integer(8));
	literals.put(new ANTLRHashString("association_rule", this), new Integer(46));
	literals.put(new ANTLRHashString("now", this), new Integer(29));
	literals.put(new ANTLRHashString("range", this), new Integer(36));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_FROM_OR_RANGE", this), new Integer(58));
	literals.put(new ANTLRHashString("slide", this), new Integer(31));
	literals.put(new ANTLRHashString("view_subtype", this), new Integer(55));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_FUNCTION_NAME", this), new Integer(64));
	literals.put(new ANTLRHashString("AND", this), new Integer(37));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_STAR", this), new Integer(61));
	literals.put(new ANTLRHashString("cluster", this), new Integer(44));
	literals.put(new ANTLRHashString("kde", this), new Integer(53));
	literals.put(new ANTLRHashString("union", this), new Integer(5));
	literals.put(new ANTLRHashString("or", this), new Integer(38));
	literals.put(new ANTLRHashString("from", this), new Integer(12));
	literals.put(new ANTLRHashString("istream", this), new Integer(10));
	literals.put(new ANTLRHashString("linearRegressionFunction", this), new Integer(43));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_QUERY", this), new Integer(57));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_BOOLEXPR", this), new Integer(56));
	literals.put(new ANTLRHashString("rstream", this), new Integer(11));
	literals.put(new ANTLRHashString("sample", this), new Integer(50));
	literals.put(new ANTLRHashString("outlier_detection", this), new Integer(48));
	literals.put(new ANTLRHashString("do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_ATTRIBUTE_NAME", this), new Integer(62));
	literals.put(new ANTLRHashString("nhc", this), new Integer(45));
	literals.put(new ANTLRHashString("not", this), new Integer(40));
}

public Token nextToken() throws TokenStreamException {
	Token theRetToken=null;
tryAgain:
	for (;;) {
		Token _token = null;
		int _ttype = Token.INVALID_TYPE;
		setCommitToPath(false);
		resetText();
		try {   // for char stream error handling
			try {   // for lexical error handling
				switch ( LA(1)) {
				case ',':
				{
					mCOMMA(true);
					theRetToken=_returnToken;
					break;
				}
				case '/':
				{
					mDIV(true);
					theRetToken=_returnToken;
					break;
				}
				case '(':
				{
					mLPAREN(true);
					theRetToken=_returnToken;
					break;
				}
				case '[':
				{
					mLSQUARE(true);
					theRetToken=_returnToken;
					break;
				}
				case '-':
				{
					mMINUS(true);
					theRetToken=_returnToken;
					break;
				}
				case '%':
				{
					mMOD(true);
					theRetToken=_returnToken;
					break;
				}
				case '*':
				{
					mMUL(true);
					theRetToken=_returnToken;
					break;
				}
				case '+':
				{
					mPLUS(true);
					theRetToken=_returnToken;
					break;
				}
				case '^':
				{
					mPOW(true);
					theRetToken=_returnToken;
					break;
				}
				case ')':
				{
					mRPAREN(true);
					theRetToken=_returnToken;
					break;
				}
				case ']':
				{
					mRSQUARE(true);
					theRetToken=_returnToken;
					break;
				}
				case ';':
				{
					mSEMI(true);
					theRetToken=_returnToken;
					break;
				}
				case '!':  case '<':  case '=':  case '>':
				{
					mPRED(true);
					theRetToken=_returnToken;
					break;
				}
				case '.':  case '0':  case '1':  case '2':
				case '3':  case '4':  case '5':  case '6':
				case '7':  case '8':  case '9':
				{
					mFlt(true);
					theRetToken=_returnToken;
					break;
				}
				case 'a':  case 'b':  case 'c':  case 'd':
				case 'e':  case 'f':  case 'g':  case 'h':
				case 'i':  case 'j':  case 'k':  case 'l':
				case 'm':  case 'n':  case 'o':  case 'p':
				case 'q':  case 'r':  case 's':  case 't':
				case 'u':  case 'v':  case 'w':  case 'x':
				case 'y':  case 'z':
				{
					mAttribute(true);
					theRetToken=_returnToken;
					break;
				}
				case '\'':
				{
					mQuotedString(true);
					theRetToken=_returnToken;
					break;
				}
				default:
				{
					if (LA(1)==EOF_CHAR) {uponEOF(); _returnToken = makeToken(Token.EOF_TYPE);}
				else {consume(); continue tryAgain;}
				}
				}
				if ( _returnToken==null ) continue tryAgain; // found SKIP token
				_ttype = _returnToken.getType();
				_ttype = testLiteralsTable(_ttype);
				_returnToken.setType(_ttype);
				return _returnToken;
			}
			catch (RecognitionException e) {
				if ( !getCommitToPath() ) {consume(); continue tryAgain;}
				throw new TokenStreamRecognitionException(e);
			}
		}
		catch (CharStreamException cse) {
			if ( cse instanceof CharStreamIOException ) {
				throw new TokenStreamIOException(((CharStreamIOException)cse).io);
			}
			else {
				throw new TokenStreamException(cse.getMessage());
			}
		}
	}
}

	public final void mCOMMA(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = COMMA;
		int _saveIndex;
		
		match(',');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mDIV(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = DIV;
		int _saveIndex;
		
		match('/');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mLPAREN(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = LPAREN;
		int _saveIndex;
		
		match('(');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mLSQUARE(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = LSQUARE;
		int _saveIndex;
		
		match('[');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mMINUS(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = MINUS;
		int _saveIndex;
		
		match('-');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mMOD(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = MOD;
		int _saveIndex;
		
		match('%');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mMUL(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = MUL;
		int _saveIndex;
		
		match('*');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mPLUS(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = PLUS;
		int _saveIndex;
		
		match('+');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mPOW(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = POW;
		int _saveIndex;
		
		match('^');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mRPAREN(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = RPAREN;
		int _saveIndex;
		
		match(')');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mRSQUARE(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = RSQUARE;
		int _saveIndex;
		
		match(']');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mSEMI(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = SEMI;
		int _saveIndex;
		
		match(';');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mPRED(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = PRED;
		int _saveIndex;
		
		{
		switch ( LA(1)) {
		case '=':
		{
			match("=");
			break;
		}
		case '!':
		{
			match("!=");
			break;
		}
		default:
			if ((LA(1)=='>') && (LA(2)=='=')) {
				match(">=");
			}
			else if ((LA(1)=='<') && (LA(2)=='=')) {
				match("<=");
			}
			else if ((LA(1)=='<') && (true)) {
				match("<");
			}
			else if ((LA(1)=='>') && (true)) {
				match(">");
			}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mFlt(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = Flt;
		int _saveIndex;
		
		boolean synPredMatched166 = false;
		if (((_tokenSet_0.member(LA(1))) && (_tokenSet_0.member(LA(2))))) {
			int _m166 = mark();
			synPredMatched166 = true;
			inputState.guessing++;
			try {
				{
				{
				_loop160:
				do {
					if (((LA(1) >= '0' && LA(1) <= '9'))) {
						matchRange('0','9');
					}
					else {
						break _loop160;
					}
					
				} while (true);
				}
				match('.');
				{
				int _cnt162=0;
				_loop162:
				do {
					if (((LA(1) >= '0' && LA(1) <= '9'))) {
						matchRange('0','9');
					}
					else {
						if ( _cnt162>=1 ) { break _loop162; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
					}
					
					_cnt162++;
				} while (true);
				}
				match('e');
				{
				switch ( LA(1)) {
				case '+':
				{
					match('+');
					break;
				}
				case '-':
				{
					match('-');
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				{
				int _cnt165=0;
				_loop165:
				do {
					if (((LA(1) >= '0' && LA(1) <= '9'))) {
						matchRange('0','9');
					}
					else {
						if ( _cnt165>=1 ) { break _loop165; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
					}
					
					_cnt165++;
				} while (true);
				}
				}
			}
			catch (RecognitionException pe) {
				synPredMatched166 = false;
			}
			rewind(_m166);
			inputState.guessing--;
		}
		if ( synPredMatched166 ) {
			{
			{
			_loop169:
			do {
				if (((LA(1) >= '0' && LA(1) <= '9'))) {
					matchRange('0','9');
				}
				else {
					break _loop169;
				}
				
			} while (true);
			}
			match('.');
			{
			int _cnt171=0;
			_loop171:
			do {
				if (((LA(1) >= '0' && LA(1) <= '9'))) {
					matchRange('0','9');
				}
				else {
					if ( _cnt171>=1 ) { break _loop171; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
				}
				
				_cnt171++;
			} while (true);
			}
			match('e');
			{
			switch ( LA(1)) {
			case '+':
			{
				match('+');
				break;
			}
			case '-':
			{
				match('-');
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			{
			int _cnt174=0;
			_loop174:
			do {
				if (((LA(1) >= '0' && LA(1) <= '9'))) {
					matchRange('0','9');
				}
				else {
					if ( _cnt174>=1 ) { break _loop174; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
				}
				
				_cnt174++;
			} while (true);
			}
			}
		}
		else {
			boolean synPredMatched180 = false;
			if (((_tokenSet_0.member(LA(1))) && (_tokenSet_0.member(LA(2))))) {
				int _m180 = mark();
				synPredMatched180 = true;
				inputState.guessing++;
				try {
					{
					{
					_loop177:
					do {
						if (((LA(1) >= '0' && LA(1) <= '9'))) {
							matchRange('0','9');
						}
						else {
							break _loop177;
						}
						
					} while (true);
					}
					match('.');
					{
					int _cnt179=0;
					_loop179:
					do {
						if (((LA(1) >= '0' && LA(1) <= '9'))) {
							matchRange('0','9');
						}
						else {
							if ( _cnt179>=1 ) { break _loop179; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
						}
						
						_cnt179++;
					} while (true);
					}
					}
				}
				catch (RecognitionException pe) {
					synPredMatched180 = false;
				}
				rewind(_m180);
				inputState.guessing--;
			}
			if ( synPredMatched180 ) {
				{
				{
				_loop183:
				do {
					if (((LA(1) >= '0' && LA(1) <= '9'))) {
						matchRange('0','9');
					}
					else {
						break _loop183;
					}
					
				} while (true);
				}
				match('.');
				{
				int _cnt185=0;
				_loop185:
				do {
					if (((LA(1) >= '0' && LA(1) <= '9'))) {
						matchRange('0','9');
					}
					else {
						if ( _cnt185>=1 ) { break _loop185; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
					}
					
					_cnt185++;
				} while (true);
				}
				}
			}
			else {
				boolean synPredMatched189 = false;
				if ((((LA(1) >= '0' && LA(1) <= '9')) && (true))) {
					int _m189 = mark();
					synPredMatched189 = true;
					inputState.guessing++;
					try {
						{
						{
						int _cnt188=0;
						_loop188:
						do {
							if (((LA(1) >= '0' && LA(1) <= '9'))) {
								matchRange('0','9');
							}
							else {
								if ( _cnt188>=1 ) { break _loop188; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
							}
							
							_cnt188++;
						} while (true);
						}
						}
					}
					catch (RecognitionException pe) {
						synPredMatched189 = false;
					}
					rewind(_m189);
					inputState.guessing--;
				}
				if ( synPredMatched189 ) {
					{
					{
					int _cnt192=0;
					_loop192:
					do {
						if (((LA(1) >= '0' && LA(1) <= '9'))) {
							matchRange('0','9');
						}
						else {
							if ( _cnt192>=1 ) { break _loop192; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
						}
						
						_cnt192++;
					} while (true);
					}
					}
					if ( inputState.guessing==0 ) {
						_ttype = Int;
					}
				}
				else {
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}}
				if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
					_token = makeToken(_ttype);
					_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
				}
				_returnToken = _token;
			}
			
	public final void mAttribute(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = Attribute;
		int _saveIndex;
		
		boolean synPredMatched199 = false;
		if ((((LA(1) >= 'a' && LA(1) <= 'z')) && (_tokenSet_1.member(LA(2))))) {
			int _m199 = mark();
			synPredMatched199 = true;
			inputState.guessing++;
			try {
				{
				matchRange('a','z');
				{
				_loop196:
				do {
					switch ( LA(1)) {
					case 'a':  case 'b':  case 'c':  case 'd':
					case 'e':  case 'f':  case 'g':  case 'h':
					case 'i':  case 'j':  case 'k':  case 'l':
					case 'm':  case 'n':  case 'o':  case 'p':
					case 'q':  case 'r':  case 's':  case 't':
					case 'u':  case 'v':  case 'w':  case 'x':
					case 'y':  case 'z':
					{
						matchRange('a','z');
						break;
					}
					case '0':  case '1':  case '2':  case '3':
					case '4':  case '5':  case '6':  case '7':
					case '8':  case '9':
					{
						matchRange('0','9');
						break;
					}
					case '_':
					{
						match('_');
						break;
					}
					default:
					{
						break _loop196;
					}
					}
				} while (true);
				}
				match('.');
				matchRange('a','z');
				{
				_loop198:
				do {
					switch ( LA(1)) {
					case 'a':  case 'b':  case 'c':  case 'd':
					case 'e':  case 'f':  case 'g':  case 'h':
					case 'i':  case 'j':  case 'k':  case 'l':
					case 'm':  case 'n':  case 'o':  case 'p':
					case 'q':  case 'r':  case 's':  case 't':
					case 'u':  case 'v':  case 'w':  case 'x':
					case 'y':  case 'z':
					{
						matchRange('a','z');
						break;
					}
					case '0':  case '1':  case '2':  case '3':
					case '4':  case '5':  case '6':  case '7':
					case '8':  case '9':
					{
						matchRange('0','9');
						break;
					}
					case '_':
					{
						match('_');
						break;
					}
					default:
					{
						break _loop198;
					}
					}
				} while (true);
				}
				}
			}
			catch (RecognitionException pe) {
				synPredMatched199 = false;
			}
			rewind(_m199);
			inputState.guessing--;
		}
		if ( synPredMatched199 ) {
			{
			matchRange('a','z');
			{
			_loop202:
			do {
				switch ( LA(1)) {
				case 'a':  case 'b':  case 'c':  case 'd':
				case 'e':  case 'f':  case 'g':  case 'h':
				case 'i':  case 'j':  case 'k':  case 'l':
				case 'm':  case 'n':  case 'o':  case 'p':
				case 'q':  case 'r':  case 's':  case 't':
				case 'u':  case 'v':  case 'w':  case 'x':
				case 'y':  case 'z':
				{
					matchRange('a','z');
					break;
				}
				case '0':  case '1':  case '2':  case '3':
				case '4':  case '5':  case '6':  case '7':
				case '8':  case '9':
				{
					matchRange('0','9');
					break;
				}
				case '_':
				{
					match('_');
					break;
				}
				default:
				{
					break _loop202;
				}
				}
			} while (true);
			}
			match('.');
			matchRange('a','z');
			{
			_loop204:
			do {
				switch ( LA(1)) {
				case 'a':  case 'b':  case 'c':  case 'd':
				case 'e':  case 'f':  case 'g':  case 'h':
				case 'i':  case 'j':  case 'k':  case 'l':
				case 'm':  case 'n':  case 'o':  case 'p':
				case 'q':  case 'r':  case 's':  case 't':
				case 'u':  case 'v':  case 'w':  case 'x':
				case 'y':  case 'z':
				{
					matchRange('a','z');
					break;
				}
				case '0':  case '1':  case '2':  case '3':
				case '4':  case '5':  case '6':  case '7':
				case '8':  case '9':
				{
					matchRange('0','9');
					break;
				}
				case '_':
				{
					match('_');
					break;
				}
				default:
				{
					break _loop204;
				}
				}
			} while (true);
			}
			}
		}
		else if (((LA(1) >= 'a' && LA(1) <= 'z')) && (true)) {
			{
			matchRange('a','z');
			{
			_loop207:
			do {
				switch ( LA(1)) {
				case 'a':  case 'b':  case 'c':  case 'd':
				case 'e':  case 'f':  case 'g':  case 'h':
				case 'i':  case 'j':  case 'k':  case 'l':
				case 'm':  case 'n':  case 'o':  case 'p':
				case 'q':  case 'r':  case 's':  case 't':
				case 'u':  case 'v':  case 'w':  case 'x':
				case 'y':  case 'z':
				{
					matchRange('a','z');
					break;
				}
				case '0':  case '1':  case '2':  case '3':
				case '4':  case '5':  case '6':  case '7':
				case '8':  case '9':
				{
					matchRange('0','9');
					break;
				}
				case '_':
				{
					match('_');
					break;
				}
				default:
				{
					break _loop207;
				}
				}
			} while (true);
			}
			}
			if ( inputState.guessing==0 ) {
				_ttype = Identifier;
			}
		}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mQuotedString(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = QuotedString;
		int _saveIndex;
		
		match('\'');
		{
		_loop210:
		do {
			if ((_tokenSet_2.member(LA(1)))) {
				matchNot('\'');
			}
			else {
				break _loop210;
			}
			
		} while (true);
		}
		match('\'');
		if ( inputState.guessing==0 ) {
			_ttype = QuotedString;
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	
	private static final long[] mk_tokenSet_0() {
		long[] data = { 288019269919178752L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());
	private static final long[] mk_tokenSet_1() {
		long[] data = { 288019269919178752L, 576460745860972544L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_1 = new BitSet(mk_tokenSet_1());
	private static final long[] mk_tokenSet_2() {
		long[] data = { -549755813889L, -1L, 0L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_2 = new BitSet(mk_tokenSet_2());
	
	}
