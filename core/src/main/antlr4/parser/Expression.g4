grammar Expression;

import Common, DataType;

expression
  : predicate
  | termExpression
  ;

predicate
  : negation
  | comparison
  | booleanLiteral
  ;

negation
  : NOT predicate
  ;

comparison
  : left=termExpression operator=EQ right=termExpression
  | left=termExpression operator=NE right=termExpression
  | left=termExpression operator=LT right=termExpression
  | left=termExpression operator=GT right=termExpression
  | left=termExpression operator=LTEQ right=termExpression
  | left=termExpression operator=GTEQ right=termExpression
  ;

termExpression
  : unaryOperator=MINUS unaryOperand=productExpression
  | left=productExpression operator=PLUS right=productExpression
  | left=productExpression operator=MINUS right=productExpression
  | productExpression
  ;

productExpression
  : left=primaryExpression operator=ASTERISK right=primaryExpression
  | left=primaryExpression operator=SLASH right=primaryExpression
  | primaryExpression
  ;

primaryExpression
  : literal
  | identifier
  | '(' expression ')'
  ;

literal
  : nullLiteral
  | booleanLiteral
  | stringLiteral
  | number
  ;

nullLiteral
  : NULL
  ;

booleanLiteral
  : TRUE
  | FALSE
  ;

number
  : tinyIntLiteral
  | smallIntLiteral
  | intLiteral
  | bigIntLiteral
  | doubleLiteral
  ;

tinyIntLiteral
  : TINYINT_LITERAL
  ;

smallIntLiteral
  : SMALLINT_LITERAL
  ;

intLiteral
  : INT_LITERAL
  ;

bigIntLiteral
  : BIGINT_LITERAL
  ;

doubleLiteral
  : DOUBLE_LITERAL
  ;

stringLiteral
  : quotedString
  ;
