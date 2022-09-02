-- Create a control table that is identical to the one in test19.dsl, but
-- without any indexes
--
-- Loads data from: data4_ctrl.csv
--
-- Create Table
create(tbl,"tbl4_ctrl",db1,4)
create(col,"col1",db1.tbl4_ctrl)
create(col,"col2",db1.tbl4_ctrl)
create(col,"col3",db1.tbl4_ctrl)
create(col,"col4",db1.tbl4_ctrl)
--
-- Load data immediately
load("my_test/data4_ctrl.csv")
--
-- Testing that the data and their indexes are durable on disk.
shutdown
