-- join test 3 - hashing many-one with larger selectivities.
-- Select + Join + aggregation
-- Performs the join using hashing
-- Query in SQL:
-- SELECT avg(tbl5_fact.col2), sum(tbl5_dim2.col2) FROM tbl5_fact,tbl5_dim2 WHERE tbl5_fact.col4=tbl5_dim2.col1 AND tbl5_fact.col2 < 160000 AND tbl5_dim2.col1<8000;
--
--
p1=select(db1.tbl5_fact.col2,null, 160000)
p2=select(db1.tbl5_dim2.col1,null, 8000)
f1=fetch(db1.tbl5_fact.col4,p1)
f2=fetch(db1.tbl5_dim2.col1,p2)
t1,t2=join(f1,p1,f2,p2,hash)
col2joined=fetch(db1.tbl5_fact.col2,t1)
col2t2joined=fetch(db1.tbl5_dim2.col2,t2)
a1=avg(col2joined)
a2=sum(col2t2joined)
print(a1,a2)
