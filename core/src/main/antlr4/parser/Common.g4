grammar Common;

import SQLLexer;

identifier
  : UNQUOTED_IDENTIFIER
  | QUOTED_IDENTIFIER
  ;

quotedString
  : QUOTED_STRING+
  ;
