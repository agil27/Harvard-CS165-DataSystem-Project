-- Creates tables for join tests
-- without any indexes
create(tbl,"tbl5_fact",db1,4)
create(col,"col1",db1.tbl5_fact)
create(col,"col2",db1.tbl5_fact)
create(col,"col3",db1.tbl5_fact)
create(col,"col4",db1.tbl5_fact)
load("my_test/data5_fact.csv")
--
create(tbl,"tbl5_dim1",db1,3)
create(col,"col1",db1.tbl5_dim1)
create(col,"col2",db1.tbl5_dim1)
create(col,"col3",db1.tbl5_dim1)
load("my_test/data5_dimension1.csv")
--
create(tbl,"tbl5_dim2",db1,2)
create(col,"col1",db1.tbl5_dim2)
create(col,"col2",db1.tbl5_dim2)
load("my_test/data5_dimension2.csv")
-- Testing that the data and their indexes are durable on disk.
shutdown
