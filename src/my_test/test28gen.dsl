-- Test for creating table with indexes
--
-- Table tbl4_clustered_btree has a clustered index with col3 being the leading column.
-- The clustered index has the form of a B-Tree.
-- The table also has a secondary sorted index.
--
-- Loads data from: data4_clustered_btree.csv
--
-- Create Table
create(tbl,"tbl4_clustered_btree",db1,4)
create(col,"col1",db1.tbl4_clustered_btree)
create(col,"col2",db1.tbl4_clustered_btree)
create(col,"col3",db1.tbl4_clustered_btree)
create(col,"col4",db1.tbl4_clustered_btree)
-- Create a clustered index on col3
create(idx,db1.tbl4_clustered_btree.col3,btree,clustered)
-- Create an unclustered btree index on col2
create(idx,db1.tbl4_clustered_btree.col2,sorted,unclustered)
--
--
-- Load data immediately in the form of a clustered index
load("/cs165/staff_test/data4_clustered_btree.csv")
--
-- Testing that the data and their indexes are durable on disk.
shutdown
