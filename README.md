# Overview

[![Build Status][travis-ci-badge]][travis-ci] [![codecov.io][codecov-badge]][codecov]

![Codecov.io][codecov-history]

[travis-ci-badge]: https://travis-ci.org/liancheng/spear.svg?branch=master
[travis-ci]: https://travis-ci.org/liancheng/spear
[codecov-badge]: https://codecov.io/github/liancheng/spear/coverage.svg?branch=master
[codecov]: https://codecov.io/github/liancheng/spear?branch=master
[codecov-history]: https://codecov.io/github/liancheng/spear/branch.svg?branch=master

This project is a sandbox and playground of mine for experimenting ideas and potential improvements to Spark SQL. It consists of:

- A parser that parsed a small SQL dialect into unrsolved logical plans
- A semantic analyzer that resolves unresolved logical plans into resolved ones
- A query optimizer that optimizes resolved query plans into equivalent but more performant ones
- A query planner that turns (optimized) logical plans into executable physical plans

Currently Spear only works with local Scala collections.

# Build

Building Spear is as easy as:

```
$ ./build/sbt package
```

# Run the REPL

Spear has an Ammonite-based REPL for interactive experiments. To start it:

```
$ ./build/sbt spear-repl/run
```

Let's create a simple DataFrame of numbers:

```scala
@ context range 10 show ()
```

```
╒══╕
│id│
├──┤
│ 0│
│ 1│
│ 2│
│ 3│
│ 4│
│ 5│
│ 6│
│ 7│
│ 8│
│ 9│
╘══╛
```

A sample query using the DataFrame API:

```scala
@ context.
    range(10).
    select('id as 'key, (rand(42) * 100) cast IntType as 'value).
    where('value % 2 === 0).
    orderBy('value.desc).
    show()
```

```
╒═══╤═════╕
│key│value│
├───┼─────┤
│  5│   90│
│  9│   78│
│  0│   72│
│  1│   68│
│  4│   66│
│  8│   46│
│  6│   36│
│  2│   30│
╘═══╧═════╛
```

Equivalent sample query using SQL:

```scala
@ context range 10 asTable 't // Registers a temporary table first

@ context.sql(
    """SELECT * FROM (
	  |  SELECT id AS key, CAST(RAND(42) * 100 AS INT) AS value FROM t
	  |) s
	  |WHERE value % 2 = 0
	  |ORDER BY value DESC
	"""
  ).show()
```

```
╒═══╤═════╕
│key│value│
├───┼─────┤
│  5│   90│
│  9│   78│
│  0│   72│
│  1│   68│
│  4│   66│
│  8│   46│
│  6│   36│
│  2│   30│
╘═══╧═════╛
```

We can also check the query plan using `explain()`:

```scala
@ context.
    range(10).
    select('id as 'key, (rand(42) * 100) cast IntType as 'value).
    where('value % 2 === 0).
    orderBy('value.desc).
    explain(true)
```

```
# Logical plan
Sort: order=[$0] ⇒ [?output?]
│ ╰╴$0: `value` DESC NULLS FIRST
╰╴Filter: condition=$0 ⇒ [?output?]
  │ ╰╴$0: ((`value` % 2:INT) = 0:INT)
  ╰╴Project: projectList=[$0, $1] ⇒ [?output?]
    │ ├╴$0: (`id` AS `key`#11)
    │ ╰╴$1: (CAST((RAND(42:INT) * 100:INT) AS INT) AS `value`#12)
    ╰╴LocalRelation: data=<local-data> ⇒ [`id`#10:BIGINT!]

# Analyzed plan
Sort: order=[$0] ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
│ ╰╴$0: `value`#12:INT! DESC NULLS FIRST
╰╴Filter: condition=$0 ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
  │ ╰╴$0: ((`value`#12:INT! % 2:INT) = 0:INT)
  ╰╴Project: projectList=[$0, $1] ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
    │ ├╴$0: (`id`#10:BIGINT! AS `key`#11)
    │ ╰╴$1: (CAST((RAND(CAST(42:INT AS BIGINT)) * CAST(100:INT AS DOUBLE)) AS INT) AS `value`#12)
    ╰╴LocalRelation: data=<local-data> ⇒ [`id`#10:BIGINT!]

# Optimized plan
Sort: order=[$0] ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
│ ╰╴$0: `value`#12:INT! DESC NULLS FIRST
╰╴Filter: condition=$0 ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
  │ ╰╴$0: ((`value`#12:INT! % 2:INT) = 0:INT)
  ╰╴Project: projectList=[$0, $1] ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
    │ ├╴$0: (`id`#10:BIGINT! AS `key`#11)
    │ ╰╴$1: (CAST((RAND(42:BIGINT) * 100.0:DOUBLE) AS INT) AS `value`#12)
    ╰╴LocalRelation: data=<local-data> ⇒ [`id`#10:BIGINT!]

# Physical plan
Sort: order=[$0] ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
│ ╰╴$0: `value`#12:INT! DESC NULLS FIRST
╰╴Filter: condition=$0 ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
  │ ╰╴$0: ((`value`#12:INT! % 2:INT) = 0:INT)
  ╰╴Project: projectList=[$0, $1] ⇒ [`key`#11:BIGINT!, `value`#12:INT!]
    │ ├╴$0: (`id`#10:BIGINT! AS `key`#11)
    │ ╰╴$1: (CAST((RAND(42:BIGINT) * 100.0:DOUBLE) AS INT) AS `value`#12)
    ╰╴LocalRelation: data=<local-data> ⇒ [`id`#10:BIGINT!]
```
