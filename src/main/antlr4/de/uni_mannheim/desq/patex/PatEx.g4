grammar PatEx;

//Rules

patex
:
	start='^'? expr=unionexp end='$'?				#union
;

unionexp
:
	concatexp '|' unionexp			#unionExpression
	| concatexp						#concat
;
concatexp
:
	repeatexp concatexp      		#concatExpression
	| repeatexp						#repeatExpression
;
repeatexp
:
	repeatexp '?'					#optionalExpression
	|repeatexp '*'					#starExpression
	|repeatexp '+'					#plusExpression
	|repeatexp '{' INT '}'          #repeatExactlyExpression
	|repeatexp '{' ',' INT '}'      #repeatMaxExpression
    |repeatexp '{' INT ',' '}'      #repeatMinExpression
	|repeatexp '{' INT ',' INT '}'  #repeatMinMaxExpression
	| simpleexp						#simpleExpression
;
simpleexp
:
	itemexp							#itemExpression
	| '[' unionexp ']'				#parens
	| '(' unionexp ')'  			#capture
;

itemexp 
:
	'.' '^'?                        #wildCard
	| item '='? '^'?                #nonWildCard
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
	'#' INT+
;

// an fid item identifier
FID :
	'#''#' INT+
;

// an string item identifier
SID :
	CHARNOHASH CHAR*
;

// a quoted string item identifier
QSID :
    SQUOTE ~('\'')* SQUOTE
    | DQUOTE ~('\"')* DQUOTE
;

fragment SQUOTE : '\'';
fragment DQUOTE : '\"';
fragment CHARNOHASH : ~('#' | '\'' | '\"' | '|' | '?' | '*' | '+' | '{' | '}' | '[' | ']' | '(' | ')' | '^' | '=' | '.'| ' ' | ',' | '\t' | '\r' | '\n') ;
fragment CHAR: ~('\'' | '\"' | '|' | '?' | '*' | '+' | '{' | '}' | '[' | ']' | '(' | ')' | '^' | '=' | '.'| ' ' | ',' | '\t' | '\r' | '\n') ;
WS  : [ \t\r\n]+ -> skip; // skip spaces, tabs, newlines
