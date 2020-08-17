# Randomized-triangle-enumeration-using-Vertica

This program allows to enumerate triangles in large graphs based on randomized algorithms using SQL on columnar DBMS Vertica

# Data set to use

The data set to use need to be in the following manner:
    
v1 <- 1 space -> v2

For example:

1 2

3 2

5 6

.

.

etc

# db_standard execution

In order to execute db_standard.py file, be sure to have vertica_db_client in use otherwise you need to modify the connection statement in the file fo feet with your vertica-python client

You need also to specify the graph orientation, whether it is directed or undireted

```
python db_standard.py path/to/Data_set directed/undirected
```
# db_random execution

In order to execute db_random.py file, be sure to have vertica_db_client in use otherwise you need to modify the connection statement in the file fo feet with your vertica-python client

Besides the path to the data set and the path to the triplet file, you need to specify the graph orientation, whether it is directed or undireted

```
python db_random.py path/to/Data_set triplet/triplet8.txt directed/undirected
```
