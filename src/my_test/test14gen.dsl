--
-- Testing for batching queries
-- Queries with no overlap
--
-- Query in SQL:
-- 10 Queries of the type:
-- SELECT col1 FROM tbl3_batch WHERE col4 >= _ AND col4 < _;
--
--
batch_queries()
s0=select(db1.tbl3_batch.col4,0,30)
s1=select(db1.tbl3_batch.col4,1000,1030)
s2=select(db1.tbl3_batch.col4,2000,2030)
s3=select(db1.tbl3_batch.col4,3000,3030)
s4=select(db1.tbl3_batch.col4,4000,4030)
s5=select(db1.tbl3_batch.col4,5000,5030)
s6=select(db1.tbl3_batch.col4,6000,6030)
s7=select(db1.tbl3_batch.col4,7000,7030)
s8=select(db1.tbl3_batch.col4,8000,8030)
s9=select(db1.tbl3_batch.col4,9000,9030)
batch_execute()
f0=fetch(db1.tbl3_batch.col1,s0)
f1=fetch(db1.tbl3_batch.col1,s1)
f2=fetch(db1.tbl3_batch.col1,s2)
f3=fetch(db1.tbl3_batch.col1,s3)
f4=fetch(db1.tbl3_batch.col1,s4)
f5=fetch(db1.tbl3_batch.col1,s5)
f6=fetch(db1.tbl3_batch.col1,s6)
f7=fetch(db1.tbl3_batch.col1,s7)
f8=fetch(db1.tbl3_batch.col1,s8)
f9=fetch(db1.tbl3_batch.col1,s9)
print(f0)
print(f1)
print(f2)
print(f3)
print(f4)
print(f5)
print(f6)
print(f7)
print(f8)
print(f9)
