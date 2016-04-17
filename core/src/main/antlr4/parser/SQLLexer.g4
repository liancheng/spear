lexer grammar SQLLexer;

ARRAY: 'ARRAY';
BIGINT: 'BIGINT';
BOOLEAN: 'BOOLEAN';
DOUBLE: 'DOUBLE';
FLOAT: 'FLOAT';
INT: 'INT';
MAP: 'MAP';
SMALLINT: 'SMALLINT';
STRING: 'STRING';
STRUCT: 'STRUCT';
TINYINT: 'TINYINT';

IDENTIFIER
  : LETTER (LETTER | DIGIT | '_')*
  ;

BACKQUOTED_IDENTIFIER
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
