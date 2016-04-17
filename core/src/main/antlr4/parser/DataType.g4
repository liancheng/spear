grammar DataType;

import SQLLexer;

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
