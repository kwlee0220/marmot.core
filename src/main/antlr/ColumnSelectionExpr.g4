grammar ColumnSelectionExpr;

@header {
package marmot.optor.support.colexpr;
}

selectionExpr : columnExpr  (',' columnExpr)*;
	
columnExpr
	: fullColName
	| fullColNameList
	| all
	| allBut
	;

all : namespace? '*';
allBut : namespace? '*' '-' '{' idList '}';
fullColNameList : namespace? '{' colNameList '}';
fullColName : namespace? colName;

colNameList : colName (',' colName)*;
colName : ID alias?;
namespace: ID '.';
alias: AS ID;
idList : ID (',' ID )*;

AS : 'AS' | 'as';
	
ID		:	ID_LETTER (ID_LETTER | DIGIT)* ;
INT 	:	DIGIT+;
FLOAT	:
			DIGIT+ '.' DIGIT*
		|	'.' DIGIT+
		;
fragment ID_LETTER :	'a'..'z'|'A'..'Z'|'_'|'/'|'$'| '\u0080'..'\ufffe' ;
fragment DIGIT :	'0'..'9' ;

STRING	:	'\'' (ESC|.)*? '\'' ;
fragment ESC :	'\\' [btnr"\\] ;

LINE_COMMENT:	'//' .*? '\n' -> skip ;
COMMENT		:	'/*' .*? '*/' -> skip ;

WS	:	[ \t\r\n]+ -> skip;