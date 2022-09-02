-- First join test - hash. Select + Join + aggregation
-- Performs the join using hashing
-- Query in SQL:
-- SELECT avg(tbl5_fact.col2), sum(tbl5_fact.col3) FROM tbl5_fact,tbl5_dim2 WHERE tbl5_fact.col4=tbl5_dim2.col1 AND tbl5_fact.col2 < 30000 AND tbl5_dim2.col1<1500;
--
--
p1=select(db1.tbl5_fact.col2,null, 30000)
p2=select(db1.tbl5_dim2.col1,null, 1500)
f1=fetch(db1.tbl5_fact.col4,p1)
f2=fetch(db1.tbl5_dim2.col1,p2)
t1,t2=join(f1,p1,f2,p2,hash)
col2joined=fetch(db1.tbl5_fact.col2,t1)
col3joined=fetch(db1.tbl5_fact.col3,t1)
a1=avg(col2joined)
a2=sum(col3joined)
print(a1,a2)
