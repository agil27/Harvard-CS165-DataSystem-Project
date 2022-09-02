-- Test for creating table with indexes
--
-- Table tbl4 has a clustered index with col3 being the leading column.
-- The clustered index has the form of a sorted column.
-- The table also has a secondary btree index.
--
-- Loads data from: data4_btree.csv
--
-- Create Table
create(tbl,"tbl4",db1,4)
create(col,"col1",db1.tbl4)
create(col,"col2",db1.tbl4)
create(col,"col3",db1.tbl4)
create(col,"col4",db1.tbl4)
-- Create a clustered index on col3
create(idx,db1.tbl4.col3,sorted,clustered)
-- Create an unclustered btree index on col2
create(idx,db1.tbl4.col2,btree,unclustered)
--
--
-- Load data immediately in the form of a clustered index
load("my_test/data4_btree.csv")
--
-- Testing that the data and their indexes are durable on disk.
shutdown
