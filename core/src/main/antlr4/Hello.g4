grammar Hello;
attribute: (ID '.')? ID ;
ID: [a-zA-Z][a-zA-Z0-9_]* ;
WS: [ \t\n\r]+ -> skip ;
