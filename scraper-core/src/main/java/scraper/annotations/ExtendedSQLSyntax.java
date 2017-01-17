package scraper.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target({
  ElementType.FIELD,
  ElementType.LOCAL_VARIABLE
})
public @interface ExtendedSQLSyntax {}
