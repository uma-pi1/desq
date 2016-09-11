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
	repeatexp concatexp 		#concatExpression
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
    INT
    | ID
    | QID
;

// an integer
INT : [0-9]+ ;

// an item identifier (excluding integers)
ID :
	CHAR+
;

// a quoted item identifier
QID :
    SQUOTE ID (WS* ID)*? SQUOTE
    | DQUOTE ID (WS* ID)*? DQUOTE
;

fragment SQUOTE : '\'';
fragment DQUOTE : '\"';
fragment CHAR : ~('\'' | '\"' | '|' | '?' | '*' | '+' | '{' | '}' | '[' | ']' | '(' | ')' | '^' | '=' | '.'| ' ' | ',' | '\t' | '\r' | '\n') ;
WS  : [ \t\r\n]+ -> skip; // skip spaces, tabs, newlines
