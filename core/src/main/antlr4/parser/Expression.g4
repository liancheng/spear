grammar Expression;

import DataType;

expression
  : literal
  ;

literal
  : NULL             #nullLiteral
  | (TRUE | FALSE)   #booleanLiteral
  | STRING_LITERAL+  #stringLiteral
  | number           #numberLiteral
  ;

number
  : TINYINT_LITERAL   #tinyIntLiteral
  | SMALLINT_LITERAL  #smallIntLiteral
  | INT_LITERAL       #intLiteral
  | BIGINT_LITERAL    #bigIntLiteral
  | DOUBLE_LITERAL    #doubleLiteral
  ;
