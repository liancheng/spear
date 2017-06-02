Spear documentation
===================

Contents:

.. toctree::
   :maxdepth: 2

Overview
--------

Spear is a SQL query engine that roots from Spark SQL. It's mostly a playground for the author to experiment and verify various ideas that may or may not be applied/applicable to Spark SQL.

Spear implements both a subset of ANSI SQL 2006 and a Scala DSL (similar to the untyped DataFrame provided by Spark SQL). Currently, Spear mostly focuses on the frontend and only provides a fairly trivial physical execution engine that only handles local Scala collections, mostly a PoC for validating the whole pipeline.

How to start
------------

Building Spear is as easy as::

  $ ./build/sbt package

Run the REPL::

  $ ./build/sbt repl
