grammar DataType;

import Common, SQLLexer;

dataType
  : primitiveType
  | complexType
  ;

primitiveType
  : name=TINYINT
  | name=SMALLINT
  | name=INT
  | name=BIGINT
  | name=FLOAT
  | name=DOUBLE
  | name=STRING
  | decimalType
  ;

decimalType
  : DECIMAL ('(' INT_LITERAL (',' INT_LITERAL)? ')')?
  ;

complexType
  : arrayType
  | mapType
  | structType
  ;

arrayType
  : ARRAY '<' elementType=dataType '>'
  ;

mapType
  : MAP '<' keyType=primitiveType ',' valueType=dataType '>'
  ;

structType
  : STRUCT '<' fields+=structField (',' fields+=structField)* '>'
  ;

structField
  : name=IDENTIFIER ':' dataType
  ;
