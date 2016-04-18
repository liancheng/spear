lexer grammar SQLLexer;

ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
ASTERISK: '*';
BIGINT: 'BIGINT';
BOOLEAN: 'BOOLEAN';
BY: 'BY';
DECIMAL: 'DECIMAL';
DESC: 'DESC';
DOUBLE: 'DOUBLE';
EQ: '=';
FALSE: 'FALSE';
FLOAT: 'FLOAT';
FROM: 'FROM';
GROUP: 'GROUP';
GT: '>';
GTEQ: '>=';
HAVING: 'HAVING';
INT: 'INT';
LT: '<';
LTEQ: '<=';
MAP: 'MAP';
MINUS: '-';
NE: '<>';
NOT: 'NOT';
NULL: 'NULL';
ORDER: 'ORDER';
PERCENT: '%';
PLUS: '+';
SELECT: 'SELECT';
SLASH: '/';
SMALLINT: 'SMALLINT';
STRING: 'STRING';
STRUCT: 'STRUCT';
TINYINT: 'TINYINT';
TRUE: 'TRUE';
WHERE: 'WHERE';

QUOTED_STRING
  : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
  | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
  ;

TINYINT_LITERAL
  : DIGIT+ 'Y'
  ;

SMALLINT_LITERAL
  : DIGIT+ 'S'
  ;

INT_LITERAL
  : DIGIT+
  ;

BIGINT_LITERAL
  : DIGIT+ 'L'
  ;

DECIMAL_LITERAL
  : DIGIT+ '.' DIGIT*
  | '.' DIGIT+
  ;

SCIENTIFIC_DECIMAL_LITERAL
  : DIGIT+ ('.' DIGIT*)? EXPONENT
  | '.' DIGIT+ EXPONENT
  ;

DOUBLE_LITERAL
  : ( INT_LITERAL
    | DECIMAL_LITERAL
    | SCIENTIFIC_DECIMAL_LITERAL
    ) 'D'
  ;

IDENTIFIER
  : UNQUOTED_IDENTIFIER
  | QUOTED_IDENTIFIER
  ;

UNQUOTED_IDENTIFIER
  : LETTER (LETTER | DIGIT | '_')*
  ;

QUOTED_IDENTIFIER
  : '`' ( ~'`' | '``' )* '`'
  ;

fragment EXPONENT
  : 'E' [+-]? DIGIT+
  ;

fragment DIGIT
  : [0-9]
  ;

fragment LETTER
  : [a-zA-Z]
  ;

SIMPLE_COMMENT
  : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
  ;

BRACKETED_COMMENT
  : '/*' .*? '*/' -> channel(HIDDEN)
  ;

WS
  : [ \r\n\t]+ -> channel(HIDDEN)
  ;

UNRECOGNIZED
  : .
  ;
