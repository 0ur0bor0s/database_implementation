# database_implementation
An implementation of a database system as a final project for a database class.  The following syntax is supported:
1)  CREATE TABLE table_name (  { column_name <data_type> [NOT NULL] }  )     <data_type> : INT, CHAR(n)
2)  DROP TABLE table_name-Drop the TPD from the DBF.
3)  LIST TABLE-List all the tables in the DBF.
4)  LIST SCHEMA FOR table_name [TO report_filename]
5)  INSERT INTO table_name VALUES (  { data_value }  )
6)  DELETE FROM table_name [ WHERE column_name <relational_operator> data_value ]-  <relation_operator> can be >, <, or =
7)  UPDATE table_name SET column = data_value [ WHERE column_name <relational_operator> data_value ]
8)  SELECT { column_name } FROM table_name [ WHERE column_name <condition> [(AND | OR) column_name <condition>] ]       
    [ ORDER BY column_name [DESC] ]
9)  SELECT aggregate(column_name) FROM table_name [ WHERE column_name <condition> [(AND | OR) column_name <condition>] ]       
    [ ORDER BY column_name [DESC] ]

# Notes
- Tokenization, list, and part of create table functionality were provided.
- Insersion, update, delete, select, drop, and writing table to disk and droping table was implemented from scratch. 
- Compiled and run on Windows 10 using gcc version 6.3.0
- support aggregates include COUNT, SUM, and AVG
