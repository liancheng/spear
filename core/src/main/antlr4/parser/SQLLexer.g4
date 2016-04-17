lexer grammar SQLLexer;

ARRAY: 'ARRAY';
BIGINT: 'BIGINT';
BOOLEAN: 'BOOLEAN';
DOUBLE: 'DOUBLE';
FALSE: 'FALSE';
FLOAT: 'FLOAT';
INT: 'INT';
MAP: 'MAP';
NULL: 'NULL';
SMALLINT: 'SMALLINT';
STRING: 'STRING';
STRUCT: 'STRUCT';
TINYINT: 'TINYINT';
TRUE: 'TRUE';

STRING_LITERAL
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
