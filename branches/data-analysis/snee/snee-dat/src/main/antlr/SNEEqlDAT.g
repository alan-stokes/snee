header
{
//updated grammar which includes CREATE statements for Data Analysis Techniques
package uk.ac.manchester.cs.snee.compiler.parser;
}

{ 
	import antlr.CommonAST; 
}

class SNEEqlParser extends Parser;

options 
{ 
	k=3; 
	buildAST=true; 
}

// Main entry rule in for a query
parse: queryExp s:SEMI^ {#s.setType(QUERY);} ;
  
queryExp:
	  query
	| unionCommand
	| ddlIntExtent
	; 

unionCommand:
		unionQuery ( UNION^ unionQuery )*
		;

unionQuery:
		LPAREN^ query RPAREN!;
	       
query    : ( DSTREAM SELECT ) =>
             DSTREAM^ queryBody
         | ( ISTREAM SELECT ) =>
             ISTREAM^ queryBody
         | ( RSTREAM SELECT ) =>
             RSTREAM^ queryBody
         //Allow select and d/i/rStream commands to be inverted by swapping them back    
         | ( SELECT DSTREAM ) =>
             dstream:SELECT^ queryBody {#dstream.setType(DSTREAM); #dstream.setText("select=dstream");}
         | ( SELECT ISTREAM ) =>
             istream:SELECT^ queryBody {#istream.setType(ISTREAM); #istream.setText("select=istream");}
         | ( SELECT RSTREAM ) =>
             rstream:SELECT^ queryBody {#rstream.setType(RSTREAM); #rstream.setText("select=rstream");}
         | ( SELECT ) => 
             queryBody;   

queryBody: select from (where)?;

select   : SELECT^ selExprs
         //Allow select and d/i/rStream commands to be inverted by swapping them back    
         | dstream:DSTREAM^ selExprs {#dstream.setType(SELECT); #dstream.setText("dstream=select");}
         | istream:ISTREAM^ selExprs {#istream.setType(SELECT); #istream.setText("istream=select");}
         | rstream:RSTREAM^ selExprs {#rstream.setType(SELECT); #rstream.setText("rstream=select");};
         
from     : FROM^ sources;

where    : WHERE^ bools;
         
         
selExprs :( MUL COMMA ) => 
            m1:MUL (COMMA! selExprs) {#m1.setType(STAR);}
         |( MUL FROM ) => 
            m2:MUL {#m2.setType(STAR);}
         | selExpr (COMMA! selExprs)?;

selExpr  :( expr AS ) => 
            expr AS^ i:Identifier {#i.setType(ATTRIBUTE_NAME);}
         | expr;

expr     : prodExpr ((PLUS^|MINUS^) prodExpr)*
         //| minus:MINUS^ pe:prodExpr { #CommonAST zero=new CommonAST();
         //                             #zero.setType(Int);
         //                             #zero.setText("0");
         //                             #minus.setFirstChild(zero);
         //                             #minus.addChild(#pe);
         //                           } 
          ; 

prodExpr : powExpr ((MUL^|DIV^|MOD^) powExpr)*; 

powExpr  : aggrExpr (POW^ aggrExpr)? ;

aggrExpr : ( Identifier LPAREN ) => 
             i:Identifier^ LPAREN! expr RPAREN! {#i.setType(FUNCTION_NAME); } 
		 | ( MIN LPAREN ) => 
             m:MIN^ LPAREN! expr RPAREN! {#m.setType(FUNCTION_NAME); }
         | atom;

atom     : ( PLUS Int ) => 
             PLUS! Int
         | ( PLUS Flt ) => 
             PLUS! Flt
         | ( MINUS Int ) => 
             m:MINUS! i:Int {#i.setText("-"+i.getText());}
         | ( MINUS Flt ) => 
             MINUS! f:Flt {#f.setText("-"+f.getText());}
         | Int
         | Flt
         | Attribute 
         | Identifier
         | LPAREN! expr RPAREN!
         ;
         
sources  : source (COMMA! source)*;

//source   : extent^ (window)?; 

source   : i:Identifier^ (window)? (Identifier)? {#i.setType(SOURCE);}
         | subQuery RPAREN^ (window)? (Identifier)?
         ;                   

subQuery : LPAREN^ query;

window   : ( LSQUARE NOW RSQUARE ) => 
             LSQUARE! NOW^ RSQUARE! 
         | ( LSQUARE NOW SLIDE ) => 
             LSQUARE! NOW^ winSlide RSQUARE!
		 | ( winFrom ) =>
		 	 winFrom (winTo)? (winSlide)? RSQUARE!
		 //winFrom includes the LSQUARE
         | LSQUARE! UNBOUNDED^ (winSlide)? RSQUARE!
         | LSQUARE! winAt (winSlide)? RSQUARE!
         | LSQUARE! winRange (winSlide)? RSQUARE! 
         | winOpen (winSlide)? RSQUARE!;
         //winOpen includes the LSQUARE 
         //omitting slide causes an input window
         //   window slide set to acquisition rate
                  
winFrom  : LSQUARE! FROM^ (NOW!)? (expr (unit))?
         //expr with from must be a literal <= 0                  
         //If unit is missing unit from winTo or winSlide used 

         //FROM assumed because of the winTo
         //expr with from must be a literal <= 0                  
         | (LSQUARE NOW expr unit winTo) =>
            LSQUARE! n1:NOW^ expr {#n1.setType(FROM); #n1.setText("from");}
         | (LSQUARE NOW expr winTo) =>
            LSQUARE! n2:NOW^ expr {#n2.setType(FROM); #n2.setText("from");}
         | (LSQUARE expr unit winTo) =>
         	l1:LSQUARE^ expr unit {#l1.setType(FROM); #l1.setText("from");}	
         | (LSQUARE expr winTo) =>
         	l2:LSQUARE^ expr {#l2.setType(FROM); #l2.setText("from");};
         
winTo    : TO^ (NOW!)? (expr (unit)?)?
         | c:COMMA^ (NOW!)? (expr (unit)?)? {#c.setType(TO); #c.setText(",=to");};
         //expr must be a literal <= 0
         //expr must be >= from's expr
         //If expr is missing 0 is assumed. 
         //      Better to just omit the to statement!
         //If unit is missing unit from winSlide used 
        
winSlide : SLIDE^ expr unit;
         //expr must be a literal > 0

winAt    : AT^ (NOW!)? (expr (unit)?)?;
         //expr must be a literal <= 0
         //If expr is missing 
         //If unit is missing unit from winSlide used 
 
winRange : RANGE^ (expr (unit)?)?;
 
winOpen  //Could be From (Expr <= 0) or Range (Expr > 0)	
         : LSQUARE! n3:NOW^ expr (unit)? {#n3.setType(FROM_OR_RANGE); #n3.setText("from/range");}
         | l3:LSQUARE^ expr (unit)? {#l3.setType(FROM_OR_RANGE); #l3.setText("from/range");}
         ;                  

unit     : i3:Identifier {#i3.setType(UNIT_NAME);}
         //Following is a example of units excepted by translator 
         //hours, hour
         //minutes, minute, min, mins				
         //seconds, second, sec, secs
         //rows, row, tuple, tuples
		 ;
                  
bools    : boolOr (AND! boolOr)*;
         
boolOr   : ( boolExpr OR ) => 
		     boolExpr OR^ boolOr
		 | boolExpr;
		 		         
boolExpr : ( LPAREN bools) =>
             LPAREN! bools RPAREN!
         | expr PRED^ expr        
         | NOT^ boolExpr
         ;


/* RULES CREATED SPECIFICALLY FOR DATs */

/* The lines below are for DAT types and specific subtypes. If you want to
add a new DAT subtype, simply go to the appropriate type that it belongs to
(e.g. classification) and add a new line with the DAT subtype name and the
parameters that it requires. The subtype name should also be added in the
tokens list that is found some lines below */

ddlIntExtent:
	createClause from;

createClause:
	CREATE^ dat Identifier;

dat:
	specificdat COMMA! outputargument RSQUARE!;

outputargument:
	Identifier;

specificdat:
	  classification
	| clustering
	| associations
	| outliers
	| sampling
	| probfns
	| views
	;	

/* Rules for Classification DATs */
classification:
	CLASSIFIER^ LSQUARE! classifier_subtype;

classifier_subtype:
	LRF^ //linear regression. It does not take any parameters
	;

/* Rules for Clustering DATs */
clustering:
	CLUSTER^ LSQUARE! cluster_subtype;

cluster_subtype:
	  NHC^ COMMA! Int		// Naive Hierarchical clustering
	;
         
/* Rules for Association rules DATs */
associations:
	ASSOCIATIONRULE^ LSQUARE! assocrule_subtype;

assocrule_subtype:
	APRIORI^ COMMA! Flt COMMA! Flt //Apriori algorithm. First param is support. Second param is confidence
	;
         
outliers:
	OUTLIER_DETECTION^ LSQUARE! outliers_subtype;

outliers_subtype:
	/* 	D3 exists in 2 forms. With 1 parameter where only the range is specified
		or with 2 parameters where we define also the probability beforehand	*/
	  D3^ COMMA! (Flt|Int) (COMMA! Flt)?	
	;

sampling:
	SAMPLE^ LSQUARE! sampling_subtype;

sampling_subtype:
	RND^ COMMA! Flt;		// random sampling
         
probfns:
	PROBFN^ LSQUARE! probfns_subtype;

probfns_subtype:
	KDE^ ;

views:
	VIEW^ LSQUARE! view_subtype;

view_subtype:
	VIEW_SUBTYPE^ ;         

class SNEEqlLexer extends Lexer;

options { k=2; filter=true;
  caseSensitive=false;
  charVocabulary='\u0000'..'\u007F';
  caseSensitiveLiterals=false;
}

tokens {
AND         = "AND";
AT          = "AT";
AS          = "AS";
//AVG         = "avg"; 
//AVERAGE     = "average";
//COUNT       = "count";
DSTREAM     = "dstream";
FROM        = "from";
//HOUR        = "hour";
//HOURS       = "hours";
ISTREAM     = "istream";
//MAX         = "max";
//MAXIMUM     = "maximum";
//MIN         = "min";
//MINS        = "mins";
//MINIMUM     = "minimum";
//MINUTE      = "minute";
//MINUTES     = "minutes";
NOT         = "not";
NOW         = "now";
OR          = "or";
RANGE       = "range";
RSTREAM     = "rstream";
//SEC         = "sec";
//SECOND      = "second";
//SECONDS     = "seconds";
//SECS        = "secs";
SELECT      = "select";
SLIDE       = "slide";
//SUM         = "sum";
TO          = "to";
UNBOUNDED   = "unbounded";
UNION		= "union";
WHERE       = "where";

/* Tokens added by Lebi to support DATs */
CREATE				= "create";	// for CREATE commands
CLASSIFIER			= "classifier";	// for classifiers
CLUSTER				= "cluster";	// for clustering
ASSOCIATIONRULE 	= "association_rule"; //for assocation rules
OUTLIER_DETECTION 	= "outlier_detection";	// for outlier detection
SAMPLE 				= "sample";		// for sampling
PROBFN	 			= "probfn";		// for probability functions
VIEW				= "view";		// for views

/* In order to support specific DAT subtypes, we also need to add the names of the subtypes */
LRF		= "linearRegressionFunction";
D3		= "d3";
APRIORI = "apriori";
NHC		= "nhc";
RND		= "random";
KDE		= "kde";
VIEW_SUBTYPE	= "view_subtype";

//These are used to retype in the parser.
//AGGREGATE = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_AGGREGATE";
BOOLEXPR  = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_BOOLEXPR";
QUERY = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_QUERY";
FROM_OR_RANGE = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_FROM_OR_RANGE";
UNIT_NAME = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_UNIT_NAME";
SOURCE = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_SOURCE";
STAR = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_STAR";
ATTRIBUTE_NAME = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_ATTRIBUTE_NAME";
LOCAL_NAME = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_LOCAL_NAME";
FUNCTION_NAME = "do_not_use_this_string_as_it_is_just_a_place_filler_to_create_a_rename_token_for_FUNCTION_NAME";
}

COMMA       : ',';
DIV         : '/';
LPAREN      : '(';
LSQUARE     : '[';
MINUS       : '-';
MOD         : '%';
MUL         : '*';
PLUS        : '+';
POW         : '^';
RPAREN      : ')';
RSQUARE     : ']';
SEMI        : ';' ;

PRED        : ("=" | "!=" | ">=" | "<" | "<=" | ">");

Flt         : ( ('0'..'9')* '.' ('0'..'9')+ 'e' ('+'|'-') ('0'..'9')+ )
              => ( ('0'..'9')* '.' ('0'..'9')+ 'e' ('+'|'-') ('0'..'9')+ )
            | ( ('0'..'9')* '.' ('0'..'9')+ )  
               => ( ('0'..'9')* '.' ('0'..'9')+ )
            | ( ('0'..'9')+ )
                => ( ('0'..'9')+ ) { $setType(Int); };

Attribute   
            : ('a' .. 'z' ( 'a' .. 'z' | '0' .. '9' | '_')* '.' 'a' .. 'z' ( 'a' .. 'z' | '0' .. '9' | '_')*)
               => ('a' .. 'z' ( 'a' .. 'z' | '0' .. '9' | '_')* '.' 'a' .. 'z' ( 'a' .. 'z' | '0' .. '9' | '_')*)
            | ( 'a' .. 'z' ( 'a' .. 'z' | '0' .. '9' | '_')* )
               { $setType(Identifier); };

{import java.lang.Math;}
class SNEEqlTreeWalker extends TreeParser;

expr returns [double r] throws ParserException
  { double a,b; r=0; }

  : #(PLUS a=expr b=expr)  { r=a+b; }
  | #(MINUS a=expr b=expr) { r=a-b; }
  | #(MUL  a=expr b=expr)  { r=a*b; }
  | #(DIV  a=expr b=expr)  { r=a/b; }
  | #(MOD  a=expr b=expr)  { r=a%b; }
  | #(POW  a=expr b=expr)  { r=Math.pow(a,b); }
  | i:Int { r=(double)Integer.parseInt(i.getText()); }  
  | f:Flt { r=(double)Float.parseFloat(f.getText()); } 
  | at:Attribute { r = ParserException.noDouble("Illegal evaluate of Attribute " + at.getText()); }
  | id:Identifier {  r = ParserException.noDouble("Illegal evaluate of Identifier " + id.getText()); }
  | fn:FUNCTION_NAME {  r = ParserException.noDouble("Illegal evaluate of FUNCTION_NAME " + fn.getText()); }
  ;

