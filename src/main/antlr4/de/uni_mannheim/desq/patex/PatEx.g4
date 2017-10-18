grammar PatEx;

//Rules

patex
:
	start='^'? expr=unionexp end='$'?	#union
;

unionexp
:
    concatexp '|' unionexp	            #unionExpression
    | concatexp					        #concat
;


concatexp
:
	unorderedexp concatexp      	    #concatExpression
	| unorderedexp					    #unordered
;

unorderedexp
:
    repeatexp '&' unorderedexp          #unorderedExpression
    | repeatexp						    #repeat
;

repeatexp
:
	repeatexp '?'					    #optionalExpression
	|repeatexp '!'? '*'					#starExpression
	|repeatexp '!'? '+'					#plusExpression
	|repeatexp '!'? '{' INT '}'         #repeatExactlyExpression
	|repeatexp '!'? '{' ',' INT '}'     #repeatMaxExpression
    |repeatexp '!'? '{' INT ',' '}'     #repeatMinExpression
	|repeatexp '!'? '{' INT ',' INT '}' #repeatMinMaxExpression
	| simpleexp						    #simpleExpression
;
simpleexp
:
	itemexp							    #itemExpression
	| '[' unionexp ']'				    #parens
	| '(' unionexp ')'  			    #capture
;

itemexp 
:
	'.' '^'?                            #wildCard
	| item '='? '^'?                    #nonWildCard
;

item
:
    INT // treated as string
    | FID
    | GID
    | SID
    | QSID
;

// an integer
INT : [0-9]+ ;

// an gid item identifier
GID :
	HASH INT+
;

// an fid item identifier
FID :
	HASH HASH INT+
;

// an string item identifier
SID :
	CHAR (CHAR|HASH)*
;

// a quoted string item identifier
QSID :
    SQUOTE ~('\'')* SQUOTE
//    | DQUOTE ~('\"')* DQUOTE
    | DQUOTE ~('"')* DQUOTE
;

fragment SQUOTE : '\'';
//fragment DQUOTE : '\"';
fragment DQUOTE : '"';
fragment HASH : '#';
//fragment CHAR: ~('#' | '\'' | '\"' | '|' | '?' | '*' | '+' | '{' | '}' | '[' | ']' | '(' | ')' | '^' | '=' | '.'| ' ' | ',' | '\t' | '\r' | '\n') ;
fragment CHAR: ~('#' | '\'' | '"' | '|' | '?' | '!' | '&' | '*' | '+' | '{' | '}' | '[' | ']' | '(' | ')' | '<' | '>' | '^' | '=' | '.'| ' ' | ',' | '\t' | '\r' | '\n') ;
WS  : [ \t\r\n]+ -> skip; // skip spaces, tabs, newlines
