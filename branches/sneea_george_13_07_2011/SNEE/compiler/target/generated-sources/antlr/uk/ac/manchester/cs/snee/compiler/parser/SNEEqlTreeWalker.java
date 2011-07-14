// $ANTLR 2.7.4: "SNEEql.g" -> "SNEEqlTreeWalker.java"$

//updated grammar which includes CREATE statements for Data Analysis Techniques
package uk.ac.manchester.cs.snee.compiler.parser;

import antlr.TreeParser;
import antlr.Token;
import antlr.collections.AST;
import antlr.RecognitionException;
import antlr.ANTLRException;
import antlr.NoViableAltException;
import antlr.MismatchedTokenException;
import antlr.SemanticException;
import antlr.collections.impl.BitSet;
import antlr.ASTPair;
import antlr.collections.impl.ASTArray;
import java.lang.Math;

public class SNEEqlTreeWalker extends antlr.TreeParser       implements SNEEqlParserTokenTypes
 {
public SNEEqlTreeWalker() {
	tokenNames = _tokenNames;
}

	public final double  expr(AST _t) throws RecognitionException, ParserException {
		double r;
		
		AST expr_AST_in = (_t == ASTNULL) ? null : (AST)_t;
		AST i = null;
		AST f = null;
		AST at = null;
		AST id = null;
		AST qs = null;
		AST fn = null;
		double a,b; r=0;
		
		try {      // for error handling
			if (_t==null) _t=ASTNULL;
			switch ( _t.getType()) {
			case PLUS:
			{
				AST __t212 = _t;
				AST tmp1_AST_in = (AST)_t;
				match(_t,PLUS);
				_t = _t.getFirstChild();
				a=expr(_t);
				_t = _retTree;
				b=expr(_t);
				_t = _retTree;
				_t = __t212;
				_t = _t.getNextSibling();
				r=a+b;
				break;
			}
			case MINUS:
			{
				AST __t213 = _t;
				AST tmp2_AST_in = (AST)_t;
				match(_t,MINUS);
				_t = _t.getFirstChild();
				a=expr(_t);
				_t = _retTree;
				b=expr(_t);
				_t = _retTree;
				_t = __t213;
				_t = _t.getNextSibling();
				r=a-b;
				break;
			}
			case MUL:
			{
				AST __t214 = _t;
				AST tmp3_AST_in = (AST)_t;
				match(_t,MUL);
				_t = _t.getFirstChild();
				a=expr(_t);
				_t = _retTree;
				b=expr(_t);
				_t = _retTree;
				_t = __t214;
				_t = _t.getNextSibling();
				r=a*b;
				break;
			}
			case DIV:
			{
				AST __t215 = _t;
				AST tmp4_AST_in = (AST)_t;
				match(_t,DIV);
				_t = _t.getFirstChild();
				a=expr(_t);
				_t = _retTree;
				b=expr(_t);
				_t = _retTree;
				_t = __t215;
				_t = _t.getNextSibling();
				r=a/b;
				break;
			}
			case MOD:
			{
				AST __t216 = _t;
				AST tmp5_AST_in = (AST)_t;
				match(_t,MOD);
				_t = _t.getFirstChild();
				a=expr(_t);
				_t = _retTree;
				b=expr(_t);
				_t = _retTree;
				_t = __t216;
				_t = _t.getNextSibling();
				r=a%b;
				break;
			}
			case POW:
			{
				AST __t217 = _t;
				AST tmp6_AST_in = (AST)_t;
				match(_t,POW);
				_t = _t.getFirstChild();
				a=expr(_t);
				_t = _retTree;
				b=expr(_t);
				_t = _retTree;
				_t = __t217;
				_t = _t.getNextSibling();
				r=Math.pow(a,b);
				break;
			}
			case Int:
			{
				i = (AST)_t;
				match(_t,Int);
				_t = _t.getNextSibling();
				r=(double)Integer.parseInt(i.getText());
				break;
			}
			case Flt:
			{
				f = (AST)_t;
				match(_t,Flt);
				_t = _t.getNextSibling();
				r=(double)Float.parseFloat(f.getText());
				break;
			}
			case Attribute:
			{
				at = (AST)_t;
				match(_t,Attribute);
				_t = _t.getNextSibling();
				r = ParserException.noDouble("Illegal evaluate of Attribute " + at.getText());
				break;
			}
			case Identifier:
			{
				id = (AST)_t;
				match(_t,Identifier);
				_t = _t.getNextSibling();
				r = ParserException.noDouble("Illegal evaluate of Identifier " + id.getText());
				break;
			}
			case QuotedString:
			{
				qs = (AST)_t;
				match(_t,QuotedString);
				_t = _t.getNextSibling();
				r = ParserException.noDouble("Illegal evaluate of QuotedString " + qs.getText());
				break;
			}
			case FUNCTION_NAME:
			{
				fn = (AST)_t;
				match(_t,FUNCTION_NAME);
				_t = _t.getNextSibling();
				r = ParserException.noDouble("Illegal evaluate of FUNCTION_NAME " + fn.getText());
				break;
			}
			default:
			{
				throw new NoViableAltException(_t);
			}
			}
		}
		catch (RecognitionException ex) {
			reportError(ex);
			if (_t!=null) {_t = _t.getNextSibling();}
		}
		_retTree = _t;
		return r;
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
	
	}
	
