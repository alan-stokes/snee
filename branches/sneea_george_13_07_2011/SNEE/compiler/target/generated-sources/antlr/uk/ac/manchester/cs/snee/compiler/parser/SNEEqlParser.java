// $ANTLR 2.7.4: "SNEEql.g" -> "SNEEqlParser.java"$

//updated grammar which includes CREATE statements for Data Analysis Techniques
package uk.ac.manchester.cs.snee.compiler.parser;

import antlr.TokenBuffer;
import antlr.TokenStreamException;
import antlr.TokenStreamIOException;
import antlr.ANTLRException;
import antlr.LLkParser;
import antlr.Token;
import antlr.TokenStream;
import antlr.RecognitionException;
import antlr.NoViableAltException;
import antlr.MismatchedTokenException;
import antlr.SemanticException;
import antlr.ParserSharedInputState;
import antlr.collections.impl.BitSet;
import antlr.collections.AST;
import java.util.Hashtable;
import antlr.ASTFactory;
import antlr.ASTPair;
import antlr.collections.impl.ASTArray;
 
	import antlr.CommonAST; 

public class SNEEqlParser extends antlr.LLkParser       implements SNEEqlParserTokenTypes
 {

protected SNEEqlParser(TokenBuffer tokenBuf, int k) {
  super(tokenBuf,k);
  tokenNames = _tokenNames;
  buildTokenTypeASTClassMap();
  astFactory = new ASTFactory(getTokenTypeToASTClassMap());
}

public SNEEqlParser(TokenBuffer tokenBuf) {
  this(tokenBuf,3);
}

protected SNEEqlParser(TokenStream lexer, int k) {
  super(lexer,k);
  tokenNames = _tokenNames;
  buildTokenTypeASTClassMap();
  astFactory = new ASTFactory(getTokenTypeToASTClassMap());
}

public SNEEqlParser(TokenStream lexer) {
  this(lexer,3);
}

public SNEEqlParser(ParserSharedInputState state) {
  super(state,3);
  tokenNames = _tokenNames;
  buildTokenTypeASTClassMap();
  astFactory = new ASTFactory(getTokenTypeToASTClassMap());
}

	public final void parse() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST parse_AST = null;
		Token  s = null;
		AST s_AST = null;
		
		try {      // for error handling
			queryExp();
			astFactory.addASTChild(currentAST, returnAST);
			s = LT(1);
			s_AST = astFactory.create(s);
			astFactory.makeASTRoot(currentAST, s_AST);
			match(SEMI);
			if ( inputState.guessing==0 ) {
				s_AST.setType(QUERY);
			}
			parse_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_0);
			} else {
			  throw ex;
			}
		}
		returnAST = parse_AST;
	}
	
	public final void queryExp() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST queryExp_AST = null;
		
		try {      // for error handling
			switch ( LA(1)) {
			case DSTREAM:
			case SELECT:
			case ISTREAM:
			case RSTREAM:
			{
				query();
				astFactory.addASTChild(currentAST, returnAST);
				queryExp_AST = (AST)currentAST.root;
				break;
			}
			case LPAREN:
			{
				unionCommand();
				astFactory.addASTChild(currentAST, returnAST);
				queryExp_AST = (AST)currentAST.root;
				break;
			}
			case CREATE:
			{
				ddlIntExtent();
				astFactory.addASTChild(currentAST, returnAST);
				queryExp_AST = (AST)currentAST.root;
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_1);
			} else {
			  throw ex;
			}
		}
		returnAST = queryExp_AST;
	}
	
	public final void query() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST query_AST = null;
		Token  dstream = null;
		AST dstream_AST = null;
		Token  istream = null;
		AST istream_AST = null;
		Token  rstream = null;
		AST rstream_AST = null;
		
		try {      // for error handling
			boolean synPredMatched9 = false;
			if (((LA(1)==DSTREAM) && ((LA(2) >= DSTREAM && LA(2) <= RSTREAM)))) {
				int _m9 = mark();
				synPredMatched9 = true;
				inputState.guessing++;
				try {
					{
					match(DSTREAM);
					match(SELECT);
					}
				}
				catch (RecognitionException pe) {
					synPredMatched9 = false;
				}
				rewind(_m9);
				inputState.guessing--;
			}
			if ( synPredMatched9 ) {
				AST tmp7_AST = null;
				tmp7_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp7_AST);
				match(DSTREAM);
				queryBody();
				astFactory.addASTChild(currentAST, returnAST);
				query_AST = (AST)currentAST.root;
			}
			else {
				boolean synPredMatched11 = false;
				if (((LA(1)==ISTREAM) && ((LA(2) >= DSTREAM && LA(2) <= RSTREAM)))) {
					int _m11 = mark();
					synPredMatched11 = true;
					inputState.guessing++;
					try {
						{
						match(ISTREAM);
						match(SELECT);
						}
					}
					catch (RecognitionException pe) {
						synPredMatched11 = false;
					}
					rewind(_m11);
					inputState.guessing--;
				}
				if ( synPredMatched11 ) {
					AST tmp8_AST = null;
					tmp8_AST = astFactory.create(LT(1));
					astFactory.makeASTRoot(currentAST, tmp8_AST);
					match(ISTREAM);
					queryBody();
					astFactory.addASTChild(currentAST, returnAST);
					query_AST = (AST)currentAST.root;
				}
				else {
					boolean synPredMatched13 = false;
					if (((LA(1)==RSTREAM) && ((LA(2) >= DSTREAM && LA(2) <= RSTREAM)))) {
						int _m13 = mark();
						synPredMatched13 = true;
						inputState.guessing++;
						try {
							{
							match(RSTREAM);
							match(SELECT);
							}
						}
						catch (RecognitionException pe) {
							synPredMatched13 = false;
						}
						rewind(_m13);
						inputState.guessing--;
					}
					if ( synPredMatched13 ) {
						AST tmp9_AST = null;
						tmp9_AST = astFactory.create(LT(1));
						astFactory.makeASTRoot(currentAST, tmp9_AST);
						match(RSTREAM);
						queryBody();
						astFactory.addASTChild(currentAST, returnAST);
						query_AST = (AST)currentAST.root;
					}
					else {
						boolean synPredMatched15 = false;
						if (((LA(1)==SELECT) && ((LA(2) >= DSTREAM && LA(2) <= RSTREAM)) && (_tokenSet_2.member(LA(3))))) {
							int _m15 = mark();
							synPredMatched15 = true;
							inputState.guessing++;
							try {
								{
								match(SELECT);
								match(DSTREAM);
								}
							}
							catch (RecognitionException pe) {
								synPredMatched15 = false;
							}
							rewind(_m15);
							inputState.guessing--;
						}
						if ( synPredMatched15 ) {
							dstream = LT(1);
							dstream_AST = astFactory.create(dstream);
							astFactory.makeASTRoot(currentAST, dstream_AST);
							match(SELECT);
							queryBody();
							astFactory.addASTChild(currentAST, returnAST);
							if ( inputState.guessing==0 ) {
								dstream_AST.setType(DSTREAM); dstream_AST.setText("select=dstream");
							}
							query_AST = (AST)currentAST.root;
						}
						else {
							boolean synPredMatched17 = false;
							if (((LA(1)==SELECT) && ((LA(2) >= DSTREAM && LA(2) <= RSTREAM)) && (_tokenSet_2.member(LA(3))))) {
								int _m17 = mark();
								synPredMatched17 = true;
								inputState.guessing++;
								try {
									{
									match(SELECT);
									match(ISTREAM);
									}
								}
								catch (RecognitionException pe) {
									synPredMatched17 = false;
								}
								rewind(_m17);
								inputState.guessing--;
							}
							if ( synPredMatched17 ) {
								istream = LT(1);
								istream_AST = astFactory.create(istream);
								astFactory.makeASTRoot(currentAST, istream_AST);
								match(SELECT);
								queryBody();
								astFactory.addASTChild(currentAST, returnAST);
								if ( inputState.guessing==0 ) {
									istream_AST.setType(ISTREAM); istream_AST.setText("select=istream");
								}
								query_AST = (AST)currentAST.root;
							}
							else {
								boolean synPredMatched19 = false;
								if (((LA(1)==SELECT) && ((LA(2) >= DSTREAM && LA(2) <= RSTREAM)) && (_tokenSet_2.member(LA(3))))) {
									int _m19 = mark();
									synPredMatched19 = true;
									inputState.guessing++;
									try {
										{
										match(SELECT);
										match(RSTREAM);
										}
									}
									catch (RecognitionException pe) {
										synPredMatched19 = false;
									}
									rewind(_m19);
									inputState.guessing--;
								}
								if ( synPredMatched19 ) {
									rstream = LT(1);
									rstream_AST = astFactory.create(rstream);
									astFactory.makeASTRoot(currentAST, rstream_AST);
									match(SELECT);
									queryBody();
									astFactory.addASTChild(currentAST, returnAST);
									if ( inputState.guessing==0 ) {
										rstream_AST.setType(RSTREAM); rstream_AST.setText("select=rstream");
									}
									query_AST = (AST)currentAST.root;
								}
								else {
									boolean synPredMatched21 = false;
									if ((((LA(1) >= DSTREAM && LA(1) <= RSTREAM)) && (_tokenSet_2.member(LA(2))))) {
										int _m21 = mark();
										synPredMatched21 = true;
										inputState.guessing++;
										try {
											{
											match(SELECT);
											}
										}
										catch (RecognitionException pe) {
											synPredMatched21 = false;
										}
										rewind(_m21);
										inputState.guessing--;
									}
									if ( synPredMatched21 ) {
										queryBody();
										astFactory.addASTChild(currentAST, returnAST);
										query_AST = (AST)currentAST.root;
									}
									else {
										throw new NoViableAltException(LT(1), getFilename());
									}
									}}}}}}
								}
								catch (RecognitionException ex) {
									if (inputState.guessing==0) {
										reportError(ex);
										consume();
										consumeUntil(_tokenSet_3);
									} else {
									  throw ex;
									}
								}
								returnAST = query_AST;
							}
							
	public final void unionCommand() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST unionCommand_AST = null;
		
		try {      // for error handling
			unionQuery();
			astFactory.addASTChild(currentAST, returnAST);
			{
			_loop5:
			do {
				if ((LA(1)==UNION)) {
					AST tmp10_AST = null;
					tmp10_AST = astFactory.create(LT(1));
					astFactory.makeASTRoot(currentAST, tmp10_AST);
					match(UNION);
					unionQuery();
					astFactory.addASTChild(currentAST, returnAST);
				}
				else {
					break _loop5;
				}
				
			} while (true);
			}
			unionCommand_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_1);
			} else {
			  throw ex;
			}
		}
		returnAST = unionCommand_AST;
	}
	
	public final void ddlIntExtent() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST ddlIntExtent_AST = null;
		
		try {      // for error handling
			createClause();
			astFactory.addASTChild(currentAST, returnAST);
			from();
			astFactory.addASTChild(currentAST, returnAST);
			ddlIntExtent_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_1);
			} else {
			  throw ex;
			}
		}
		returnAST = ddlIntExtent_AST;
	}
	
	public final void unionQuery() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST unionQuery_AST = null;
		
		try {      // for error handling
			AST tmp11_AST = null;
			tmp11_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp11_AST);
			match(LPAREN);
			query();
			astFactory.addASTChild(currentAST, returnAST);
			match(RPAREN);
			unionQuery_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_4);
			} else {
			  throw ex;
			}
		}
		returnAST = unionQuery_AST;
	}
	
	public final void queryBody() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST queryBody_AST = null;
		
		try {      // for error handling
			select();
			astFactory.addASTChild(currentAST, returnAST);
			from();
			astFactory.addASTChild(currentAST, returnAST);
			{
			switch ( LA(1)) {
			case WHERE:
			{
				where();
				astFactory.addASTChild(currentAST, returnAST);
				break;
			}
			case SEMI:
			case RPAREN:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			queryBody_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_3);
			} else {
			  throw ex;
			}
		}
		returnAST = queryBody_AST;
	}
	
	public final void select() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST select_AST = null;
		Token  dstream = null;
		AST dstream_AST = null;
		Token  istream = null;
		AST istream_AST = null;
		Token  rstream = null;
		AST rstream_AST = null;
		
		try {      // for error handling
			switch ( LA(1)) {
			case SELECT:
			{
				AST tmp13_AST = null;
				tmp13_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp13_AST);
				match(SELECT);
				selExprs();
				astFactory.addASTChild(currentAST, returnAST);
				select_AST = (AST)currentAST.root;
				break;
			}
			case DSTREAM:
			{
				dstream = LT(1);
				dstream_AST = astFactory.create(dstream);
				astFactory.makeASTRoot(currentAST, dstream_AST);
				match(DSTREAM);
				selExprs();
				astFactory.addASTChild(currentAST, returnAST);
				if ( inputState.guessing==0 ) {
					dstream_AST.setType(SELECT); dstream_AST.setText("dstream=select");
				}
				select_AST = (AST)currentAST.root;
				break;
			}
			case ISTREAM:
			{
				istream = LT(1);
				istream_AST = astFactory.create(istream);
				astFactory.makeASTRoot(currentAST, istream_AST);
				match(ISTREAM);
				selExprs();
				astFactory.addASTChild(currentAST, returnAST);
				if ( inputState.guessing==0 ) {
					istream_AST.setType(SELECT); istream_AST.setText("istream=select");
				}
				select_AST = (AST)currentAST.root;
				break;
			}
			case RSTREAM:
			{
				rstream = LT(1);
				rstream_AST = astFactory.create(rstream);
				astFactory.makeASTRoot(currentAST, rstream_AST);
				match(RSTREAM);
				selExprs();
				astFactory.addASTChild(currentAST, returnAST);
				if ( inputState.guessing==0 ) {
					rstream_AST.setType(SELECT); rstream_AST.setText("rstream=select");
				}
				select_AST = (AST)currentAST.root;
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_5);
			} else {
			  throw ex;
			}
		}
		returnAST = select_AST;
	}
	
	public final void from() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST from_AST = null;
		
		try {      // for error handling
			AST tmp14_AST = null;
			tmp14_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp14_AST);
			match(FROM);
			sources();
			astFactory.addASTChild(currentAST, returnAST);
			from_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_6);
			} else {
			  throw ex;
			}
		}
		returnAST = from_AST;
	}
	
	public final void where() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST where_AST = null;
		
		try {      // for error handling
			AST tmp15_AST = null;
			tmp15_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp15_AST);
			match(WHERE);
			bools();
			astFactory.addASTChild(currentAST, returnAST);
			where_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_3);
			} else {
			  throw ex;
			}
		}
		returnAST = where_AST;
	}
	
	public final void selExprs() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST selExprs_AST = null;
		Token  m1 = null;
		AST m1_AST = null;
		Token  m2 = null;
		AST m2_AST = null;
		
		try {      // for error handling
			boolean synPredMatched29 = false;
			if (((LA(1)==MUL) && (LA(2)==COMMA))) {
				int _m29 = mark();
				synPredMatched29 = true;
				inputState.guessing++;
				try {
					{
					match(MUL);
					match(COMMA);
					}
				}
				catch (RecognitionException pe) {
					synPredMatched29 = false;
				}
				rewind(_m29);
				inputState.guessing--;
			}
			if ( synPredMatched29 ) {
				m1 = LT(1);
				m1_AST = astFactory.create(m1);
				astFactory.addASTChild(currentAST, m1_AST);
				match(MUL);
				{
				match(COMMA);
				selExprs();
				astFactory.addASTChild(currentAST, returnAST);
				}
				if ( inputState.guessing==0 ) {
					m1_AST.setType(STAR);
				}
				selExprs_AST = (AST)currentAST.root;
			}
			else {
				boolean synPredMatched32 = false;
				if (((LA(1)==MUL) && (LA(2)==FROM))) {
					int _m32 = mark();
					synPredMatched32 = true;
					inputState.guessing++;
					try {
						{
						match(MUL);
						match(FROM);
						}
					}
					catch (RecognitionException pe) {
						synPredMatched32 = false;
					}
					rewind(_m32);
					inputState.guessing--;
				}
				if ( synPredMatched32 ) {
					m2 = LT(1);
					m2_AST = astFactory.create(m2);
					astFactory.addASTChild(currentAST, m2_AST);
					match(MUL);
					if ( inputState.guessing==0 ) {
						m2_AST.setType(STAR);
					}
					selExprs_AST = (AST)currentAST.root;
				}
				else if ((_tokenSet_7.member(LA(1)))) {
					selExpr();
					astFactory.addASTChild(currentAST, returnAST);
					{
					switch ( LA(1)) {
					case COMMA:
					{
						match(COMMA);
						selExprs();
						astFactory.addASTChild(currentAST, returnAST);
						break;
					}
					case FROM:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					selExprs_AST = (AST)currentAST.root;
				}
				else {
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
			}
			catch (RecognitionException ex) {
				if (inputState.guessing==0) {
					reportError(ex);
					consume();
					consumeUntil(_tokenSet_5);
				} else {
				  throw ex;
				}
			}
			returnAST = selExprs_AST;
		}
		
	public final void sources() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST sources_AST = null;
		
		try {      // for error handling
			source();
			astFactory.addASTChild(currentAST, returnAST);
			{
			_loop63:
			do {
				if ((LA(1)==COMMA)) {
					match(COMMA);
					source();
					astFactory.addASTChild(currentAST, returnAST);
				}
				else {
					break _loop63;
				}
				
			} while (true);
			}
			sources_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_6);
			} else {
			  throw ex;
			}
		}
		returnAST = sources_AST;
	}
	
	public final void bools() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST bools_AST = null;
		
		try {      // for error handling
			boolOr();
			astFactory.addASTChild(currentAST, returnAST);
			{
			_loop115:
			do {
				if ((LA(1)==AND)) {
					match(AND);
					boolOr();
					astFactory.addASTChild(currentAST, returnAST);
				}
				else {
					break _loop115;
				}
				
			} while (true);
			}
			bools_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_3);
			} else {
			  throw ex;
			}
		}
		returnAST = bools_AST;
	}
	
	public final void selExpr() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST selExpr_AST = null;
		Token  i = null;
		AST i_AST = null;
		
		try {      // for error handling
			boolean synPredMatched36 = false;
			if (((_tokenSet_7.member(LA(1))) && (_tokenSet_8.member(LA(2))) && (_tokenSet_9.member(LA(3))))) {
				int _m36 = mark();
				synPredMatched36 = true;
				inputState.guessing++;
				try {
					{
					expr();
					match(AS);
					}
				}
				catch (RecognitionException pe) {
					synPredMatched36 = false;
				}
				rewind(_m36);
				inputState.guessing--;
			}
			if ( synPredMatched36 ) {
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				AST tmp20_AST = null;
				tmp20_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp20_AST);
				match(AS);
				i = LT(1);
				i_AST = astFactory.create(i);
				astFactory.addASTChild(currentAST, i_AST);
				match(Identifier);
				if ( inputState.guessing==0 ) {
					i_AST.setType(ATTRIBUTE_NAME);
				}
				selExpr_AST = (AST)currentAST.root;
			}
			else if ((_tokenSet_7.member(LA(1))) && (_tokenSet_10.member(LA(2))) && (_tokenSet_11.member(LA(3)))) {
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				selExpr_AST = (AST)currentAST.root;
			}
			else {
				throw new NoViableAltException(LT(1), getFilename());
			}
			
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_12);
			} else {
			  throw ex;
			}
		}
		returnAST = selExpr_AST;
	}
	
	public final void expr() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST expr_AST = null;
		
		try {      // for error handling
			prodExpr();
			astFactory.addASTChild(currentAST, returnAST);
			{
			_loop40:
			do {
				if ((LA(1)==PLUS||LA(1)==MINUS)) {
					{
					switch ( LA(1)) {
					case PLUS:
					{
						AST tmp21_AST = null;
						tmp21_AST = astFactory.create(LT(1));
						astFactory.makeASTRoot(currentAST, tmp21_AST);
						match(PLUS);
						break;
					}
					case MINUS:
					{
						AST tmp22_AST = null;
						tmp22_AST = astFactory.create(LT(1));
						astFactory.makeASTRoot(currentAST, tmp22_AST);
						match(MINUS);
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					prodExpr();
					astFactory.addASTChild(currentAST, returnAST);
				}
				else {
					break _loop40;
				}
				
			} while (true);
			}
			expr_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_13);
			} else {
			  throw ex;
			}
		}
		returnAST = expr_AST;
	}
	
	public final void prodExpr() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST prodExpr_AST = null;
		
		try {      // for error handling
			powExpr();
			astFactory.addASTChild(currentAST, returnAST);
			{
			_loop44:
			do {
				if ((LA(1)==MUL||LA(1)==DIV||LA(1)==MOD)) {
					{
					switch ( LA(1)) {
					case MUL:
					{
						AST tmp23_AST = null;
						tmp23_AST = astFactory.create(LT(1));
						astFactory.makeASTRoot(currentAST, tmp23_AST);
						match(MUL);
						break;
					}
					case DIV:
					{
						AST tmp24_AST = null;
						tmp24_AST = astFactory.create(LT(1));
						astFactory.makeASTRoot(currentAST, tmp24_AST);
						match(DIV);
						break;
					}
					case MOD:
					{
						AST tmp25_AST = null;
						tmp25_AST = astFactory.create(LT(1));
						astFactory.makeASTRoot(currentAST, tmp25_AST);
						match(MOD);
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					powExpr();
					astFactory.addASTChild(currentAST, returnAST);
				}
				else {
					break _loop44;
				}
				
			} while (true);
			}
			prodExpr_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_14);
			} else {
			  throw ex;
			}
		}
		returnAST = prodExpr_AST;
	}
	
	public final void powExpr() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST powExpr_AST = null;
		
		try {      // for error handling
			aggrExpr();
			astFactory.addASTChild(currentAST, returnAST);
			{
			switch ( LA(1)) {
			case POW:
			{
				AST tmp26_AST = null;
				tmp26_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp26_AST);
				match(POW);
				aggrExpr();
				astFactory.addASTChild(currentAST, returnAST);
				break;
			}
			case EOF:
			case SEMI:
			case RPAREN:
			case FROM:
			case MUL:
			case COMMA:
			case AS:
			case Identifier:
			case PLUS:
			case MINUS:
			case DIV:
			case MOD:
			case RSQUARE:
			case SLIDE:
			case TO:
			case AND:
			case OR:
			case PRED:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			powExpr_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_15);
			} else {
			  throw ex;
			}
		}
		returnAST = powExpr_AST;
	}
	
	public final void aggrExpr() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST aggrExpr_AST = null;
		Token  i = null;
		AST i_AST = null;
		Token  m = null;
		AST m_AST = null;
		
		try {      // for error handling
			boolean synPredMatched49 = false;
			if (((LA(1)==Identifier) && (LA(2)==LPAREN) && (_tokenSet_7.member(LA(3))))) {
				int _m49 = mark();
				synPredMatched49 = true;
				inputState.guessing++;
				try {
					{
					match(Identifier);
					match(LPAREN);
					}
				}
				catch (RecognitionException pe) {
					synPredMatched49 = false;
				}
				rewind(_m49);
				inputState.guessing--;
			}
			if ( synPredMatched49 ) {
				i = LT(1);
				i_AST = astFactory.create(i);
				astFactory.makeASTRoot(currentAST, i_AST);
				match(Identifier);
				match(LPAREN);
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				match(RPAREN);
				if ( inputState.guessing==0 ) {
					i_AST.setType(FUNCTION_NAME);
				}
				aggrExpr_AST = (AST)currentAST.root;
			}
			else {
				boolean synPredMatched51 = false;
				if (((LA(1)==MIN))) {
					int _m51 = mark();
					synPredMatched51 = true;
					inputState.guessing++;
					try {
						{
						match(MIN);
						match(LPAREN);
						}
					}
					catch (RecognitionException pe) {
						synPredMatched51 = false;
					}
					rewind(_m51);
					inputState.guessing--;
				}
				if ( synPredMatched51 ) {
					m = LT(1);
					m_AST = astFactory.create(m);
					astFactory.makeASTRoot(currentAST, m_AST);
					match(MIN);
					match(LPAREN);
					expr();
					astFactory.addASTChild(currentAST, returnAST);
					match(RPAREN);
					if ( inputState.guessing==0 ) {
						m_AST.setType(FUNCTION_NAME);
					}
					aggrExpr_AST = (AST)currentAST.root;
				}
				else if ((_tokenSet_16.member(LA(1))) && (_tokenSet_17.member(LA(2))) && (_tokenSet_18.member(LA(3)))) {
					atom();
					astFactory.addASTChild(currentAST, returnAST);
					aggrExpr_AST = (AST)currentAST.root;
				}
				else {
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
			}
			catch (RecognitionException ex) {
				if (inputState.guessing==0) {
					reportError(ex);
					consume();
					consumeUntil(_tokenSet_19);
				} else {
				  throw ex;
				}
			}
			returnAST = aggrExpr_AST;
		}
		
	public final void atom() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST atom_AST = null;
		Token  m = null;
		AST m_AST = null;
		Token  i = null;
		AST i_AST = null;
		Token  f = null;
		AST f_AST = null;
		
		try {      // for error handling
			switch ( LA(1)) {
			case Int:
			{
				AST tmp31_AST = null;
				tmp31_AST = astFactory.create(LT(1));
				astFactory.addASTChild(currentAST, tmp31_AST);
				match(Int);
				atom_AST = (AST)currentAST.root;
				break;
			}
			case Flt:
			{
				AST tmp32_AST = null;
				tmp32_AST = astFactory.create(LT(1));
				astFactory.addASTChild(currentAST, tmp32_AST);
				match(Flt);
				atom_AST = (AST)currentAST.root;
				break;
			}
			case Attribute:
			{
				AST tmp33_AST = null;
				tmp33_AST = astFactory.create(LT(1));
				astFactory.addASTChild(currentAST, tmp33_AST);
				match(Attribute);
				atom_AST = (AST)currentAST.root;
				break;
			}
			case Identifier:
			{
				AST tmp34_AST = null;
				tmp34_AST = astFactory.create(LT(1));
				astFactory.addASTChild(currentAST, tmp34_AST);
				match(Identifier);
				atom_AST = (AST)currentAST.root;
				break;
			}
			case QuotedString:
			{
				AST tmp35_AST = null;
				tmp35_AST = astFactory.create(LT(1));
				astFactory.addASTChild(currentAST, tmp35_AST);
				match(QuotedString);
				atom_AST = (AST)currentAST.root;
				break;
			}
			case LPAREN:
			{
				match(LPAREN);
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				match(RPAREN);
				atom_AST = (AST)currentAST.root;
				break;
			}
			default:
				boolean synPredMatched54 = false;
				if (((LA(1)==PLUS) && (LA(2)==Int))) {
					int _m54 = mark();
					synPredMatched54 = true;
					inputState.guessing++;
					try {
						{
						match(PLUS);
						match(Int);
						}
					}
					catch (RecognitionException pe) {
						synPredMatched54 = false;
					}
					rewind(_m54);
					inputState.guessing--;
				}
				if ( synPredMatched54 ) {
					match(PLUS);
					AST tmp39_AST = null;
					tmp39_AST = astFactory.create(LT(1));
					astFactory.addASTChild(currentAST, tmp39_AST);
					match(Int);
					atom_AST = (AST)currentAST.root;
				}
				else {
					boolean synPredMatched56 = false;
					if (((LA(1)==PLUS) && (LA(2)==Flt))) {
						int _m56 = mark();
						synPredMatched56 = true;
						inputState.guessing++;
						try {
							{
							match(PLUS);
							match(Flt);
							}
						}
						catch (RecognitionException pe) {
							synPredMatched56 = false;
						}
						rewind(_m56);
						inputState.guessing--;
					}
					if ( synPredMatched56 ) {
						match(PLUS);
						AST tmp41_AST = null;
						tmp41_AST = astFactory.create(LT(1));
						astFactory.addASTChild(currentAST, tmp41_AST);
						match(Flt);
						atom_AST = (AST)currentAST.root;
					}
					else {
						boolean synPredMatched58 = false;
						if (((LA(1)==MINUS) && (LA(2)==Int))) {
							int _m58 = mark();
							synPredMatched58 = true;
							inputState.guessing++;
							try {
								{
								match(MINUS);
								match(Int);
								}
							}
							catch (RecognitionException pe) {
								synPredMatched58 = false;
							}
							rewind(_m58);
							inputState.guessing--;
						}
						if ( synPredMatched58 ) {
							m = LT(1);
							m_AST = astFactory.create(m);
							match(MINUS);
							i = LT(1);
							i_AST = astFactory.create(i);
							astFactory.addASTChild(currentAST, i_AST);
							match(Int);
							if ( inputState.guessing==0 ) {
								i_AST.setText("-"+i.getText());
							}
							atom_AST = (AST)currentAST.root;
						}
						else {
							boolean synPredMatched60 = false;
							if (((LA(1)==MINUS) && (LA(2)==Flt))) {
								int _m60 = mark();
								synPredMatched60 = true;
								inputState.guessing++;
								try {
									{
									match(MINUS);
									match(Flt);
									}
								}
								catch (RecognitionException pe) {
									synPredMatched60 = false;
								}
								rewind(_m60);
								inputState.guessing--;
							}
							if ( synPredMatched60 ) {
								match(MINUS);
								f = LT(1);
								f_AST = astFactory.create(f);
								astFactory.addASTChild(currentAST, f_AST);
								match(Flt);
								if ( inputState.guessing==0 ) {
									f_AST.setText("-"+f.getText());
								}
								atom_AST = (AST)currentAST.root;
							}
						else {
							throw new NoViableAltException(LT(1), getFilename());
						}
						}}}}
					}
					catch (RecognitionException ex) {
						if (inputState.guessing==0) {
							reportError(ex);
							consume();
							consumeUntil(_tokenSet_19);
						} else {
						  throw ex;
						}
					}
					returnAST = atom_AST;
				}
				
	public final void source() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST source_AST = null;
		Token  i = null;
		AST i_AST = null;
		
		try {      // for error handling
			switch ( LA(1)) {
			case Identifier:
			{
				i = LT(1);
				i_AST = astFactory.create(i);
				astFactory.makeASTRoot(currentAST, i_AST);
				match(Identifier);
				{
				switch ( LA(1)) {
				case LSQUARE:
				{
					window();
					astFactory.addASTChild(currentAST, returnAST);
					break;
				}
				case SEMI:
				case RPAREN:
				case WHERE:
				case COMMA:
				case Identifier:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				{
				switch ( LA(1)) {
				case Identifier:
				{
					AST tmp43_AST = null;
					tmp43_AST = astFactory.create(LT(1));
					astFactory.addASTChild(currentAST, tmp43_AST);
					match(Identifier);
					break;
				}
				case SEMI:
				case RPAREN:
				case WHERE:
				case COMMA:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				if ( inputState.guessing==0 ) {
					i_AST.setType(SOURCE);
				}
				source_AST = (AST)currentAST.root;
				break;
			}
			case LPAREN:
			{
				subQuery();
				astFactory.addASTChild(currentAST, returnAST);
				AST tmp44_AST = null;
				tmp44_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp44_AST);
				match(RPAREN);
				{
				switch ( LA(1)) {
				case LSQUARE:
				{
					window();
					astFactory.addASTChild(currentAST, returnAST);
					break;
				}
				case SEMI:
				case RPAREN:
				case WHERE:
				case COMMA:
				case Identifier:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				{
				switch ( LA(1)) {
				case Identifier:
				{
					AST tmp45_AST = null;
					tmp45_AST = astFactory.create(LT(1));
					astFactory.addASTChild(currentAST, tmp45_AST);
					match(Identifier);
					break;
				}
				case SEMI:
				case RPAREN:
				case WHERE:
				case COMMA:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				source_AST = (AST)currentAST.root;
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_20);
			} else {
			  throw ex;
			}
		}
		returnAST = source_AST;
	}
	
	public final void window() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST window_AST = null;
		
		try {      // for error handling
			boolean synPredMatched72 = false;
			if (((LA(1)==LSQUARE) && (LA(2)==NOW) && (LA(3)==RSQUARE))) {
				int _m72 = mark();
				synPredMatched72 = true;
				inputState.guessing++;
				try {
					{
					match(LSQUARE);
					match(NOW);
					match(RSQUARE);
					}
				}
				catch (RecognitionException pe) {
					synPredMatched72 = false;
				}
				rewind(_m72);
				inputState.guessing--;
			}
			if ( synPredMatched72 ) {
				match(LSQUARE);
				AST tmp47_AST = null;
				tmp47_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp47_AST);
				match(NOW);
				match(RSQUARE);
				window_AST = (AST)currentAST.root;
			}
			else {
				boolean synPredMatched74 = false;
				if (((LA(1)==LSQUARE) && (LA(2)==NOW) && (LA(3)==SLIDE))) {
					int _m74 = mark();
					synPredMatched74 = true;
					inputState.guessing++;
					try {
						{
						match(LSQUARE);
						match(NOW);
						match(SLIDE);
						}
					}
					catch (RecognitionException pe) {
						synPredMatched74 = false;
					}
					rewind(_m74);
					inputState.guessing--;
				}
				if ( synPredMatched74 ) {
					match(LSQUARE);
					AST tmp50_AST = null;
					tmp50_AST = astFactory.create(LT(1));
					astFactory.makeASTRoot(currentAST, tmp50_AST);
					match(NOW);
					winSlide();
					astFactory.addASTChild(currentAST, returnAST);
					match(RSQUARE);
					window_AST = (AST)currentAST.root;
				}
				else {
					boolean synPredMatched76 = false;
					if (((LA(1)==LSQUARE) && (_tokenSet_21.member(LA(2))) && (_tokenSet_22.member(LA(3))))) {
						int _m76 = mark();
						synPredMatched76 = true;
						inputState.guessing++;
						try {
							{
							winFrom();
							}
						}
						catch (RecognitionException pe) {
							synPredMatched76 = false;
						}
						rewind(_m76);
						inputState.guessing--;
					}
					if ( synPredMatched76 ) {
						winFrom();
						astFactory.addASTChild(currentAST, returnAST);
						{
						switch ( LA(1)) {
						case COMMA:
						case TO:
						{
							winTo();
							astFactory.addASTChild(currentAST, returnAST);
							break;
						}
						case RSQUARE:
						case SLIDE:
						{
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						{
						switch ( LA(1)) {
						case SLIDE:
						{
							winSlide();
							astFactory.addASTChild(currentAST, returnAST);
							break;
						}
						case RSQUARE:
						{
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						match(RSQUARE);
						window_AST = (AST)currentAST.root;
					}
					else if ((LA(1)==LSQUARE) && (LA(2)==UNBOUNDED)) {
						match(LSQUARE);
						AST tmp54_AST = null;
						tmp54_AST = astFactory.create(LT(1));
						astFactory.makeASTRoot(currentAST, tmp54_AST);
						match(UNBOUNDED);
						{
						switch ( LA(1)) {
						case SLIDE:
						{
							winSlide();
							astFactory.addASTChild(currentAST, returnAST);
							break;
						}
						case RSQUARE:
						{
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						match(RSQUARE);
						window_AST = (AST)currentAST.root;
					}
					else if ((LA(1)==LSQUARE) && (LA(2)==AT)) {
						match(LSQUARE);
						winAt();
						astFactory.addASTChild(currentAST, returnAST);
						{
						switch ( LA(1)) {
						case SLIDE:
						{
							winSlide();
							astFactory.addASTChild(currentAST, returnAST);
							break;
						}
						case RSQUARE:
						{
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						match(RSQUARE);
						window_AST = (AST)currentAST.root;
					}
					else if ((LA(1)==LSQUARE) && (LA(2)==RANGE)) {
						match(LSQUARE);
						winRange();
						astFactory.addASTChild(currentAST, returnAST);
						{
						switch ( LA(1)) {
						case SLIDE:
						{
							winSlide();
							astFactory.addASTChild(currentAST, returnAST);
							break;
						}
						case RSQUARE:
						{
							break;
						}
						default:
						{
							throw new NoViableAltException(LT(1), getFilename());
						}
						}
						}
						match(RSQUARE);
						window_AST = (AST)currentAST.root;
					}
					else if ((LA(1)==LSQUARE) && (LA(2)==RESCAN)) {
						match(LSQUARE);
						AST tmp61_AST = null;
						tmp61_AST = astFactory.create(LT(1));
						astFactory.makeASTRoot(currentAST, tmp61_AST);
						match(RESCAN);
						expr();
						astFactory.addASTChild(currentAST, returnAST);
						unit();
						astFactory.addASTChild(currentAST, returnAST);
						match(RSQUARE);
						window_AST = (AST)currentAST.root;
					}
					else {
						throw new NoViableAltException(LT(1), getFilename());
					}
					}}
				}
				catch (RecognitionException ex) {
					if (inputState.guessing==0) {
						reportError(ex);
						consume();
						consumeUntil(_tokenSet_23);
					} else {
					  throw ex;
					}
				}
				returnAST = window_AST;
			}
			
	public final void subQuery() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST subQuery_AST = null;
		
		try {      // for error handling
			AST tmp63_AST = null;
			tmp63_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp63_AST);
			match(LPAREN);
			query();
			astFactory.addASTChild(currentAST, returnAST);
			subQuery_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_24);
			} else {
			  throw ex;
			}
		}
		returnAST = subQuery_AST;
	}
	
	public final void winSlide() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST winSlide_AST = null;
		
		try {      // for error handling
			AST tmp64_AST = null;
			tmp64_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp64_AST);
			match(SLIDE);
			expr();
			astFactory.addASTChild(currentAST, returnAST);
			unit();
			astFactory.addASTChild(currentAST, returnAST);
			winSlide_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_25);
			} else {
			  throw ex;
			}
		}
		returnAST = winSlide_AST;
	}
	
	public final void winFrom() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST winFrom_AST = null;
		Token  n1 = null;
		AST n1_AST = null;
		Token  n2 = null;
		AST n2_AST = null;
		Token  l1 = null;
		AST l1_AST = null;
		Token  l2 = null;
		AST l2_AST = null;
		
		try {      // for error handling
			if ((LA(1)==LSQUARE) && (LA(2)==FROM)) {
				match(LSQUARE);
				AST tmp66_AST = null;
				tmp66_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp66_AST);
				match(FROM);
				{
				switch ( LA(1)) {
				case NOW:
				{
					match(NOW);
					break;
				}
				case LPAREN:
				case COMMA:
				case Identifier:
				case PLUS:
				case MINUS:
				case MIN:
				case Int:
				case Flt:
				case Attribute:
				case QuotedString:
				case RSQUARE:
				case SLIDE:
				case TO:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				{
				switch ( LA(1)) {
				case LPAREN:
				case Identifier:
				case PLUS:
				case MINUS:
				case MIN:
				case Int:
				case Flt:
				case Attribute:
				case QuotedString:
				{
					expr();
					astFactory.addASTChild(currentAST, returnAST);
					{
					unit();
					astFactory.addASTChild(currentAST, returnAST);
					}
					break;
				}
				case COMMA:
				case RSQUARE:
				case SLIDE:
				case TO:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				winFrom_AST = (AST)currentAST.root;
			}
			else {
				boolean synPredMatched87 = false;
				if (((LA(1)==LSQUARE) && (LA(2)==NOW) && (_tokenSet_7.member(LA(3))))) {
					int _m87 = mark();
					synPredMatched87 = true;
					inputState.guessing++;
					try {
						{
						match(LSQUARE);
						match(NOW);
						expr();
						unit();
						winTo();
						}
					}
					catch (RecognitionException pe) {
						synPredMatched87 = false;
					}
					rewind(_m87);
					inputState.guessing--;
				}
				if ( synPredMatched87 ) {
					match(LSQUARE);
					n1 = LT(1);
					n1_AST = astFactory.create(n1);
					astFactory.makeASTRoot(currentAST, n1_AST);
					match(NOW);
					expr();
					astFactory.addASTChild(currentAST, returnAST);
					if ( inputState.guessing==0 ) {
						n1_AST.setType(FROM); n1_AST.setText("from");
					}
					winFrom_AST = (AST)currentAST.root;
				}
				else {
					boolean synPredMatched89 = false;
					if (((LA(1)==LSQUARE) && (LA(2)==NOW) && (_tokenSet_7.member(LA(3))))) {
						int _m89 = mark();
						synPredMatched89 = true;
						inputState.guessing++;
						try {
							{
							match(LSQUARE);
							match(NOW);
							expr();
							winTo();
							}
						}
						catch (RecognitionException pe) {
							synPredMatched89 = false;
						}
						rewind(_m89);
						inputState.guessing--;
					}
					if ( synPredMatched89 ) {
						match(LSQUARE);
						n2 = LT(1);
						n2_AST = astFactory.create(n2);
						astFactory.makeASTRoot(currentAST, n2_AST);
						match(NOW);
						expr();
						astFactory.addASTChild(currentAST, returnAST);
						if ( inputState.guessing==0 ) {
							n2_AST.setType(FROM); n2_AST.setText("from");
						}
						winFrom_AST = (AST)currentAST.root;
					}
					else {
						boolean synPredMatched91 = false;
						if (((LA(1)==LSQUARE) && (_tokenSet_7.member(LA(2))) && (_tokenSet_26.member(LA(3))))) {
							int _m91 = mark();
							synPredMatched91 = true;
							inputState.guessing++;
							try {
								{
								match(LSQUARE);
								expr();
								unit();
								winTo();
								}
							}
							catch (RecognitionException pe) {
								synPredMatched91 = false;
							}
							rewind(_m91);
							inputState.guessing--;
						}
						if ( synPredMatched91 ) {
							l1 = LT(1);
							l1_AST = astFactory.create(l1);
							astFactory.makeASTRoot(currentAST, l1_AST);
							match(LSQUARE);
							expr();
							astFactory.addASTChild(currentAST, returnAST);
							unit();
							astFactory.addASTChild(currentAST, returnAST);
							if ( inputState.guessing==0 ) {
								l1_AST.setType(FROM); l1_AST.setText("from");
							}
							winFrom_AST = (AST)currentAST.root;
						}
						else {
							boolean synPredMatched93 = false;
							if (((LA(1)==LSQUARE) && (_tokenSet_7.member(LA(2))) && (_tokenSet_27.member(LA(3))))) {
								int _m93 = mark();
								synPredMatched93 = true;
								inputState.guessing++;
								try {
									{
									match(LSQUARE);
									expr();
									winTo();
									}
								}
								catch (RecognitionException pe) {
									synPredMatched93 = false;
								}
								rewind(_m93);
								inputState.guessing--;
							}
							if ( synPredMatched93 ) {
								l2 = LT(1);
								l2_AST = astFactory.create(l2);
								astFactory.makeASTRoot(currentAST, l2_AST);
								match(LSQUARE);
								expr();
								astFactory.addASTChild(currentAST, returnAST);
								if ( inputState.guessing==0 ) {
									l2_AST.setType(FROM); l2_AST.setText("from");
								}
								winFrom_AST = (AST)currentAST.root;
							}
							else {
								throw new NoViableAltException(LT(1), getFilename());
							}
							}}}}
						}
						catch (RecognitionException ex) {
							if (inputState.guessing==0) {
								reportError(ex);
								consume();
								consumeUntil(_tokenSet_28);
							} else {
							  throw ex;
							}
						}
						returnAST = winFrom_AST;
					}
					
	public final void winTo() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST winTo_AST = null;
		Token  c = null;
		AST c_AST = null;
		
		try {      // for error handling
			switch ( LA(1)) {
			case TO:
			{
				AST tmp70_AST = null;
				tmp70_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp70_AST);
				match(TO);
				{
				switch ( LA(1)) {
				case NOW:
				{
					match(NOW);
					break;
				}
				case LPAREN:
				case Identifier:
				case PLUS:
				case MINUS:
				case MIN:
				case Int:
				case Flt:
				case Attribute:
				case QuotedString:
				case RSQUARE:
				case SLIDE:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				{
				switch ( LA(1)) {
				case LPAREN:
				case Identifier:
				case PLUS:
				case MINUS:
				case MIN:
				case Int:
				case Flt:
				case Attribute:
				case QuotedString:
				{
					expr();
					astFactory.addASTChild(currentAST, returnAST);
					{
					switch ( LA(1)) {
					case Identifier:
					{
						unit();
						astFactory.addASTChild(currentAST, returnAST);
						break;
					}
					case RSQUARE:
					case SLIDE:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					break;
				}
				case RSQUARE:
				case SLIDE:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				winTo_AST = (AST)currentAST.root;
				break;
			}
			case COMMA:
			{
				c = LT(1);
				c_AST = astFactory.create(c);
				astFactory.makeASTRoot(currentAST, c_AST);
				match(COMMA);
				{
				switch ( LA(1)) {
				case NOW:
				{
					match(NOW);
					break;
				}
				case LPAREN:
				case Identifier:
				case PLUS:
				case MINUS:
				case MIN:
				case Int:
				case Flt:
				case Attribute:
				case QuotedString:
				case RSQUARE:
				case SLIDE:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				{
				switch ( LA(1)) {
				case LPAREN:
				case Identifier:
				case PLUS:
				case MINUS:
				case MIN:
				case Int:
				case Flt:
				case Attribute:
				case QuotedString:
				{
					expr();
					astFactory.addASTChild(currentAST, returnAST);
					{
					switch ( LA(1)) {
					case Identifier:
					{
						unit();
						astFactory.addASTChild(currentAST, returnAST);
						break;
					}
					case RSQUARE:
					case SLIDE:
					{
						break;
					}
					default:
					{
						throw new NoViableAltException(LT(1), getFilename());
					}
					}
					}
					break;
				}
				case RSQUARE:
				case SLIDE:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				if ( inputState.guessing==0 ) {
					c_AST.setType(TO); c_AST.setText(",=to");
				}
				winTo_AST = (AST)currentAST.root;
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_29);
			} else {
			  throw ex;
			}
		}
		returnAST = winTo_AST;
	}
	
	public final void winAt() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST winAt_AST = null;
		
		try {      // for error handling
			AST tmp73_AST = null;
			tmp73_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp73_AST);
			match(AT);
			{
			switch ( LA(1)) {
			case NOW:
			{
				match(NOW);
				break;
			}
			case LPAREN:
			case Identifier:
			case PLUS:
			case MINUS:
			case MIN:
			case Int:
			case Flt:
			case Attribute:
			case QuotedString:
			case RSQUARE:
			case SLIDE:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			switch ( LA(1)) {
			case LPAREN:
			case Identifier:
			case PLUS:
			case MINUS:
			case MIN:
			case Int:
			case Flt:
			case Attribute:
			case QuotedString:
			{
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				{
				switch ( LA(1)) {
				case Identifier:
				{
					unit();
					astFactory.addASTChild(currentAST, returnAST);
					break;
				}
				case RSQUARE:
				case SLIDE:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				break;
			}
			case RSQUARE:
			case SLIDE:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			winAt_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_29);
			} else {
			  throw ex;
			}
		}
		returnAST = winAt_AST;
	}
	
	public final void winRange() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST winRange_AST = null;
		
		try {      // for error handling
			AST tmp75_AST = null;
			tmp75_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp75_AST);
			match(RANGE);
			{
			switch ( LA(1)) {
			case LPAREN:
			case Identifier:
			case PLUS:
			case MINUS:
			case MIN:
			case Int:
			case Flt:
			case Attribute:
			case QuotedString:
			{
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				{
				switch ( LA(1)) {
				case Identifier:
				{
					unit();
					astFactory.addASTChild(currentAST, returnAST);
					break;
				}
				case RSQUARE:
				case SLIDE:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				break;
			}
			case RSQUARE:
			case SLIDE:
			{
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			winRange_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_29);
			} else {
			  throw ex;
			}
		}
		returnAST = winRange_AST;
	}
	
	public final void unit() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST unit_AST = null;
		Token  i3 = null;
		AST i3_AST = null;
		
		try {      // for error handling
			i3 = LT(1);
			i3_AST = astFactory.create(i3);
			astFactory.addASTChild(currentAST, i3_AST);
			match(Identifier);
			if ( inputState.guessing==0 ) {
				i3_AST.setType(UNIT_NAME);
			}
			unit_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_30);
			} else {
			  throw ex;
			}
		}
		returnAST = unit_AST;
	}
	
	public final void winOpen() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST winOpen_AST = null;
		Token  n3 = null;
		AST n3_AST = null;
		Token  l3 = null;
		AST l3_AST = null;
		
		try {      // for error handling
			if ((LA(1)==LSQUARE) && (LA(2)==NOW)) {
				match(LSQUARE);
				n3 = LT(1);
				n3_AST = astFactory.create(n3);
				astFactory.makeASTRoot(currentAST, n3_AST);
				match(NOW);
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				{
				switch ( LA(1)) {
				case Identifier:
				{
					unit();
					astFactory.addASTChild(currentAST, returnAST);
					break;
				}
				case EOF:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				if ( inputState.guessing==0 ) {
					n3_AST.setType(FROM_OR_RANGE); n3_AST.setText("from/range");
				}
				winOpen_AST = (AST)currentAST.root;
			}
			else if ((LA(1)==LSQUARE) && (_tokenSet_7.member(LA(2)))) {
				l3 = LT(1);
				l3_AST = astFactory.create(l3);
				astFactory.makeASTRoot(currentAST, l3_AST);
				match(LSQUARE);
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				{
				switch ( LA(1)) {
				case Identifier:
				{
					unit();
					astFactory.addASTChild(currentAST, returnAST);
					break;
				}
				case EOF:
				{
					break;
				}
				default:
				{
					throw new NoViableAltException(LT(1), getFilename());
				}
				}
				}
				if ( inputState.guessing==0 ) {
					l3_AST.setType(FROM_OR_RANGE); l3_AST.setText("from/range");
				}
				winOpen_AST = (AST)currentAST.root;
			}
			else {
				throw new NoViableAltException(LT(1), getFilename());
			}
			
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_0);
			} else {
			  throw ex;
			}
		}
		returnAST = winOpen_AST;
	}
	
	public final void boolOr() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST boolOr_AST = null;
		
		try {      // for error handling
			boolean synPredMatched118 = false;
			if (((_tokenSet_31.member(LA(1))) && (_tokenSet_32.member(LA(2))) && (_tokenSet_33.member(LA(3))))) {
				int _m118 = mark();
				synPredMatched118 = true;
				inputState.guessing++;
				try {
					{
					boolExpr();
					match(OR);
					}
				}
				catch (RecognitionException pe) {
					synPredMatched118 = false;
				}
				rewind(_m118);
				inputState.guessing--;
			}
			if ( synPredMatched118 ) {
				boolExpr();
				astFactory.addASTChild(currentAST, returnAST);
				AST tmp77_AST = null;
				tmp77_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp77_AST);
				match(OR);
				boolOr();
				astFactory.addASTChild(currentAST, returnAST);
				boolOr_AST = (AST)currentAST.root;
			}
			else if ((_tokenSet_31.member(LA(1))) && (_tokenSet_32.member(LA(2))) && (_tokenSet_33.member(LA(3)))) {
				boolExpr();
				astFactory.addASTChild(currentAST, returnAST);
				boolOr_AST = (AST)currentAST.root;
			}
			else {
				throw new NoViableAltException(LT(1), getFilename());
			}
			
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_34);
			} else {
			  throw ex;
			}
		}
		returnAST = boolOr_AST;
	}
	
	public final void boolExpr() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST boolExpr_AST = null;
		
		try {      // for error handling
			boolean synPredMatched121 = false;
			if (((LA(1)==LPAREN) && (_tokenSet_31.member(LA(2))) && (_tokenSet_32.member(LA(3))))) {
				int _m121 = mark();
				synPredMatched121 = true;
				inputState.guessing++;
				try {
					{
					match(LPAREN);
					bools();
					}
				}
				catch (RecognitionException pe) {
					synPredMatched121 = false;
				}
				rewind(_m121);
				inputState.guessing--;
			}
			if ( synPredMatched121 ) {
				match(LPAREN);
				bools();
				astFactory.addASTChild(currentAST, returnAST);
				match(RPAREN);
				boolExpr_AST = (AST)currentAST.root;
			}
			else if ((_tokenSet_7.member(LA(1))) && (_tokenSet_35.member(LA(2))) && (_tokenSet_36.member(LA(3)))) {
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				AST tmp80_AST = null;
				tmp80_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp80_AST);
				match(PRED);
				expr();
				astFactory.addASTChild(currentAST, returnAST);
				boolExpr_AST = (AST)currentAST.root;
			}
			else if ((LA(1)==NOT)) {
				AST tmp81_AST = null;
				tmp81_AST = astFactory.create(LT(1));
				astFactory.makeASTRoot(currentAST, tmp81_AST);
				match(NOT);
				boolExpr();
				astFactory.addASTChild(currentAST, returnAST);
				boolExpr_AST = (AST)currentAST.root;
			}
			else {
				throw new NoViableAltException(LT(1), getFilename());
			}
			
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_37);
			} else {
			  throw ex;
			}
		}
		returnAST = boolExpr_AST;
	}
	
	public final void createClause() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST createClause_AST = null;
		
		try {      // for error handling
			AST tmp82_AST = null;
			tmp82_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp82_AST);
			match(CREATE);
			dat();
			astFactory.addASTChild(currentAST, returnAST);
			AST tmp83_AST = null;
			tmp83_AST = astFactory.create(LT(1));
			astFactory.addASTChild(currentAST, tmp83_AST);
			match(Identifier);
			createClause_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_5);
			} else {
			  throw ex;
			}
		}
		returnAST = createClause_AST;
	}
	
	public final void dat() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST dat_AST = null;
		
		try {      // for error handling
			specificdat();
			astFactory.addASTChild(currentAST, returnAST);
			match(COMMA);
			outputargument();
			astFactory.addASTChild(currentAST, returnAST);
			match(RSQUARE);
			dat_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_38);
			} else {
			  throw ex;
			}
		}
		returnAST = dat_AST;
	}
	
	public final void specificdat() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST specificdat_AST = null;
		
		try {      // for error handling
			switch ( LA(1)) {
			case CLASSIFIER:
			{
				classification();
				astFactory.addASTChild(currentAST, returnAST);
				specificdat_AST = (AST)currentAST.root;
				break;
			}
			case CLUSTER:
			{
				clustering();
				astFactory.addASTChild(currentAST, returnAST);
				specificdat_AST = (AST)currentAST.root;
				break;
			}
			case ASSOCIATIONRULE:
			{
				associations();
				astFactory.addASTChild(currentAST, returnAST);
				specificdat_AST = (AST)currentAST.root;
				break;
			}
			case OUTLIER_DETECTION:
			{
				outliers();
				astFactory.addASTChild(currentAST, returnAST);
				specificdat_AST = (AST)currentAST.root;
				break;
			}
			case SAMPLE:
			{
				sampling();
				astFactory.addASTChild(currentAST, returnAST);
				specificdat_AST = (AST)currentAST.root;
				break;
			}
			case PROBFN:
			{
				probfns();
				astFactory.addASTChild(currentAST, returnAST);
				specificdat_AST = (AST)currentAST.root;
				break;
			}
			case VIEW:
			{
				views();
				astFactory.addASTChild(currentAST, returnAST);
				specificdat_AST = (AST)currentAST.root;
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = specificdat_AST;
	}
	
	public final void outputargument() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST outputargument_AST = null;
		
		try {      // for error handling
			AST tmp86_AST = null;
			tmp86_AST = astFactory.create(LT(1));
			astFactory.addASTChild(currentAST, tmp86_AST);
			match(Identifier);
			outputargument_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_25);
			} else {
			  throw ex;
			}
		}
		returnAST = outputargument_AST;
	}
	
	public final void classification() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST classification_AST = null;
		
		try {      // for error handling
			AST tmp87_AST = null;
			tmp87_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp87_AST);
			match(CLASSIFIER);
			match(LSQUARE);
			classifier_subtype();
			astFactory.addASTChild(currentAST, returnAST);
			classification_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = classification_AST;
	}
	
	public final void clustering() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST clustering_AST = null;
		
		try {      // for error handling
			AST tmp89_AST = null;
			tmp89_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp89_AST);
			match(CLUSTER);
			match(LSQUARE);
			cluster_subtype();
			astFactory.addASTChild(currentAST, returnAST);
			clustering_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = clustering_AST;
	}
	
	public final void associations() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST associations_AST = null;
		
		try {      // for error handling
			AST tmp91_AST = null;
			tmp91_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp91_AST);
			match(ASSOCIATIONRULE);
			match(LSQUARE);
			assocrule_subtype();
			astFactory.addASTChild(currentAST, returnAST);
			associations_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = associations_AST;
	}
	
	public final void outliers() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST outliers_AST = null;
		
		try {      // for error handling
			AST tmp93_AST = null;
			tmp93_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp93_AST);
			match(OUTLIER_DETECTION);
			match(LSQUARE);
			outliers_subtype();
			astFactory.addASTChild(currentAST, returnAST);
			outliers_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = outliers_AST;
	}
	
	public final void sampling() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST sampling_AST = null;
		
		try {      // for error handling
			AST tmp95_AST = null;
			tmp95_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp95_AST);
			match(SAMPLE);
			match(LSQUARE);
			sampling_subtype();
			astFactory.addASTChild(currentAST, returnAST);
			sampling_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = sampling_AST;
	}
	
	public final void probfns() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST probfns_AST = null;
		
		try {      // for error handling
			AST tmp97_AST = null;
			tmp97_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp97_AST);
			match(PROBFN);
			match(LSQUARE);
			probfns_subtype();
			astFactory.addASTChild(currentAST, returnAST);
			probfns_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = probfns_AST;
	}
	
	public final void views() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST views_AST = null;
		
		try {      // for error handling
			AST tmp99_AST = null;
			tmp99_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp99_AST);
			match(VIEW);
			match(LSQUARE);
			view_subtype();
			astFactory.addASTChild(currentAST, returnAST);
			views_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = views_AST;
	}
	
	public final void classifier_subtype() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST classifier_subtype_AST = null;
		
		try {      // for error handling
			AST tmp101_AST = null;
			tmp101_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp101_AST);
			match(LRF);
			classifier_subtype_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = classifier_subtype_AST;
	}
	
	public final void cluster_subtype() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST cluster_subtype_AST = null;
		
		try {      // for error handling
			AST tmp102_AST = null;
			tmp102_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp102_AST);
			match(NHC);
			match(COMMA);
			AST tmp104_AST = null;
			tmp104_AST = astFactory.create(LT(1));
			astFactory.addASTChild(currentAST, tmp104_AST);
			match(Int);
			cluster_subtype_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = cluster_subtype_AST;
	}
	
	public final void assocrule_subtype() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST assocrule_subtype_AST = null;
		
		try {      // for error handling
			AST tmp105_AST = null;
			tmp105_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp105_AST);
			match(APRIORI);
			match(COMMA);
			AST tmp107_AST = null;
			tmp107_AST = astFactory.create(LT(1));
			astFactory.addASTChild(currentAST, tmp107_AST);
			match(Flt);
			match(COMMA);
			AST tmp109_AST = null;
			tmp109_AST = astFactory.create(LT(1));
			astFactory.addASTChild(currentAST, tmp109_AST);
			match(Flt);
			assocrule_subtype_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = assocrule_subtype_AST;
	}
	
	public final void outliers_subtype() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST outliers_subtype_AST = null;
		
		try {      // for error handling
			AST tmp110_AST = null;
			tmp110_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp110_AST);
			match(D3);
			match(COMMA);
			{
			switch ( LA(1)) {
			case Flt:
			{
				AST tmp112_AST = null;
				tmp112_AST = astFactory.create(LT(1));
				astFactory.addASTChild(currentAST, tmp112_AST);
				match(Flt);
				break;
			}
			case Int:
			{
				AST tmp113_AST = null;
				tmp113_AST = astFactory.create(LT(1));
				astFactory.addASTChild(currentAST, tmp113_AST);
				match(Int);
				break;
			}
			default:
			{
				throw new NoViableAltException(LT(1), getFilename());
			}
			}
			}
			{
			if ((LA(1)==COMMA) && (LA(2)==Flt)) {
				match(COMMA);
				AST tmp115_AST = null;
				tmp115_AST = astFactory.create(LT(1));
				astFactory.addASTChild(currentAST, tmp115_AST);
				match(Flt);
			}
			else if ((LA(1)==COMMA) && (LA(2)==Identifier)) {
			}
			else {
				throw new NoViableAltException(LT(1), getFilename());
			}
			
			}
			outliers_subtype_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = outliers_subtype_AST;
	}
	
	public final void sampling_subtype() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST sampling_subtype_AST = null;
		
		try {      // for error handling
			AST tmp116_AST = null;
			tmp116_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp116_AST);
			match(RND);
			match(COMMA);
			AST tmp118_AST = null;
			tmp118_AST = astFactory.create(LT(1));
			astFactory.addASTChild(currentAST, tmp118_AST);
			match(Flt);
			sampling_subtype_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = sampling_subtype_AST;
	}
	
	public final void probfns_subtype() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST probfns_subtype_AST = null;
		
		try {      // for error handling
			AST tmp119_AST = null;
			tmp119_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp119_AST);
			match(KDE);
			probfns_subtype_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = probfns_subtype_AST;
	}
	
	public final void view_subtype() throws RecognitionException, TokenStreamException {
		
		returnAST = null;
		ASTPair currentAST = new ASTPair();
		AST view_subtype_AST = null;
		
		try {      // for error handling
			AST tmp120_AST = null;
			tmp120_AST = astFactory.create(LT(1));
			astFactory.makeASTRoot(currentAST, tmp120_AST);
			match(VIEW_SUBTYPE);
			view_subtype_AST = (AST)currentAST.root;
		}
		catch (RecognitionException ex) {
			if (inputState.guessing==0) {
				reportError(ex);
				consume();
				consumeUntil(_tokenSet_39);
			} else {
			  throw ex;
			}
		}
		returnAST = view_subtype_AST;
	}
	
	
	public static final String[] _tokenNames = {
		"<0>",
		"EOF",
		"<2>",
		"NULL_TREE_LOOKAHEAD",
		"SEMI",
		"\"union\"",
		"LPAREN",
		"RPAREN",
		"\"dstream\"",
		"\"select\"",
		"\"istream\"",
		"\"rstream\"",
		"\"from\"",
		"\"where\"",
		"MUL",
		"COMMA",
		"\"AS\"",
		"Identifier",
		"PLUS",
		"MINUS",
		"DIV",
		"MOD",
		"POW",
		"MIN",
		"Int",
		"Flt",
		"Attribute",
		"QuotedString",
		"LSQUARE",
		"\"now\"",
		"RSQUARE",
		"\"slide\"",
		"\"unbounded\"",
		"\"rescan\"",
		"\"to\"",
		"\"AT\"",
		"\"range\"",
		"\"AND\"",
		"\"or\"",
		"PRED",
		"\"not\"",
		"\"create\"",
		"\"classifier\"",
		"\"linearRegressionFunction\"",
		"\"cluster\"",
		"\"nhc\"",
		"\"association_rule\"",
		"\"apriori\"",
		"\"outlier_detection\"",
		"\"d3\"",
		"\"sample\"",
		"\"random\"",
		"\"probfn\"",
		"\"kde\"",
		"\"view\"",
		"\"view_subtype\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_BOOLEXPR\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_QUERY\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_FROM_OR_RANGE\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_UNIT_NAME\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_SOURCE\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_STAR\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_ATTRIBUTE_NAME\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_LOCAL_NAME\"",
		"\"do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_FUNCTION_NAME\""
	};
	
	protected void buildTokenTypeASTClassMap() {
		tokenTypeToASTClassMap=null;
	};
	
	private static final long[] mk_tokenSet_0() {
		long[] data = { 2L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());
	private static final long[] mk_tokenSet_1() {
		long[] data = { 16L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_1 = new BitSet(mk_tokenSet_1());
	private static final long[] mk_tokenSet_2() {
		long[] data = { 260980800L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_2 = new BitSet(mk_tokenSet_2());
	private static final long[] mk_tokenSet_3() {
		long[] data = { 144L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_3 = new BitSet(mk_tokenSet_3());
	private static final long[] mk_tokenSet_4() {
		long[] data = { 48L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_4 = new BitSet(mk_tokenSet_4());
	private static final long[] mk_tokenSet_5() {
		long[] data = { 4096L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_5 = new BitSet(mk_tokenSet_5());
	private static final long[] mk_tokenSet_6() {
		long[] data = { 8336L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_6 = new BitSet(mk_tokenSet_6());
	private static final long[] mk_tokenSet_7() {
		long[] data = { 260964416L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_7 = new BitSet(mk_tokenSet_7());
	private static final long[] mk_tokenSet_8() {
		long[] data = { 268386368L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_8 = new BitSet(mk_tokenSet_8());
	private static final long[] mk_tokenSet_9() {
		long[] data = { 268386496L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_9 = new BitSet(mk_tokenSet_9());
	private static final long[] mk_tokenSet_10() {
		long[] data = { 268357696L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_10 = new BitSet(mk_tokenSet_10());
	private static final long[] mk_tokenSet_11() {
		long[] data = { 268357824L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_11 = new BitSet(mk_tokenSet_11());
	private static final long[] mk_tokenSet_12() {
		long[] data = { 36864L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_12 = new BitSet(mk_tokenSet_12());
	private static final long[] mk_tokenSet_13() {
		long[] data = { 982474002578L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_13 = new BitSet(mk_tokenSet_13());
	private static final long[] mk_tokenSet_14() {
		long[] data = { 982474789010L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_14 = new BitSet(mk_tokenSet_14());
	private static final long[] mk_tokenSet_15() {
		long[] data = { 982477951122L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_15 = new BitSet(mk_tokenSet_15());
	private static final long[] mk_tokenSet_16() {
		long[] data = { 252575808L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_16 = new BitSet(mk_tokenSet_16());
	private static final long[] mk_tokenSet_17() {
		long[] data = { 982742192338L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_17 = new BitSet(mk_tokenSet_17());
	private static final long[] mk_tokenSet_18() {
		long[] data = { 2083059134706L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_18 = new BitSet(mk_tokenSet_18());
	private static final long[] mk_tokenSet_19() {
		long[] data = { 982482145426L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_19 = new BitSet(mk_tokenSet_19());
	private static final long[] mk_tokenSet_20() {
		long[] data = { 41104L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_20 = new BitSet(mk_tokenSet_20());
	private static final long[] mk_tokenSet_21() {
		long[] data = { 797839424L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_21 = new BitSet(mk_tokenSet_21());
	private static final long[] mk_tokenSet_22() {
		long[] data = { 21206319168L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_22 = new BitSet(mk_tokenSet_22());
	private static final long[] mk_tokenSet_23() {
		long[] data = { 172176L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_23 = new BitSet(mk_tokenSet_23());
	private static final long[] mk_tokenSet_24() {
		long[] data = { 128L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_24 = new BitSet(mk_tokenSet_24());
	private static final long[] mk_tokenSet_25() {
		long[] data = { 1073741824L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_25 = new BitSet(mk_tokenSet_25());
	private static final long[] mk_tokenSet_26() {
		long[] data = { 268320832L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_26 = new BitSet(mk_tokenSet_26());
	private static final long[] mk_tokenSet_27() {
		long[] data = { 20669448256L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_27 = new BitSet(mk_tokenSet_27());
	private static final long[] mk_tokenSet_28() {
		long[] data = { 20401127424L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_28 = new BitSet(mk_tokenSet_28());
	private static final long[] mk_tokenSet_29() {
		long[] data = { 3221225472L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_29 = new BitSet(mk_tokenSet_29());
	private static final long[] mk_tokenSet_30() {
		long[] data = { 20401127426L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_30 = new BitSet(mk_tokenSet_30());
	private static final long[] mk_tokenSet_31() {
		long[] data = { 1099772592192L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_31 = new BitSet(mk_tokenSet_31());
	private static final long[] mk_tokenSet_32() {
		long[] data = { 1649535762496L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_32 = new BitSet(mk_tokenSet_32());
	private static final long[] mk_tokenSet_33() {
		long[] data = { 1649535762624L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_33 = new BitSet(mk_tokenSet_33());
	private static final long[] mk_tokenSet_34() {
		long[] data = { 137438953616L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_34 = new BitSet(mk_tokenSet_34());
	private static final long[] mk_tokenSet_35() {
		long[] data = { 550024134720L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_35 = new BitSet(mk_tokenSet_35());
	private static final long[] mk_tokenSet_36() {
		long[] data = { 550024134848L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_36 = new BitSet(mk_tokenSet_36());
	private static final long[] mk_tokenSet_37() {
		long[] data = { 412316860560L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_37 = new BitSet(mk_tokenSet_37());
	private static final long[] mk_tokenSet_38() {
		long[] data = { 131072L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_38 = new BitSet(mk_tokenSet_38());
	private static final long[] mk_tokenSet_39() {
		long[] data = { 32768L, 0L};
		return data;
	}
	public static final BitSet _tokenSet_39 = new BitSet(mk_tokenSet_39());
	
	}
