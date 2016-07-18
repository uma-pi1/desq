grammar PatEx;

@header {
    package de.uni_mannheim.desq.patex;
}


//Rules

patex
:
	unionexp						#union
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
	|repeatexp '{' WORD '}'          #repeatMaxExpression
	|repeatexp '{' WORD ',' '}'      #repeatMinExpression
	|repeatexp '{' WORD ',' WORD '}'  #repeatMinMaxExpression
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
	| WORD '='? '^'?                 #item
;

WORD :
	CHAR+
;  


CHAR : ~('|' | '?' | '*' | '+' | '{' | '}' | '[' | ']' | '(' | ')' | '^' | '=' | '.'| ' ' | ',' | '\t' | '\r' | '\n') ;
INT : [0-9]+ ;
WS  : [ \t\r\n]+ -> skip; // skip spaces, tabs, newlines
