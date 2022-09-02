--
-- Testing for batching queries
-- Queries with full overlap (subsumption)
--
-- Query in SQL:
-- 10 Queries of the type:
-- SELECT col1 FROM tbl3_batch WHERE col4 >= _ AND col4 < _;
--
--
batch_queries()
s0=select(db1.tbl3_batch.col4,5014,5074)
s1=select(db1.tbl3_batch.col4,5016,5072)
s2=select(db1.tbl3_batch.col4,5018,5070)
s3=select(db1.tbl3_batch.col4,5020,5068)
s4=select(db1.tbl3_batch.col4,5022,5066)
s5=select(db1.tbl3_batch.col4,5024,5064)
s6=select(db1.tbl3_batch.col4,5026,5062)
s7=select(db1.tbl3_batch.col4,5028,5060)
s8=select(db1.tbl3_batch.col4,5030,5058)
s9=select(db1.tbl3_batch.col4,5032,5056)
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
