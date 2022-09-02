-- Join test 2 - nested-loop. Select + Join + aggregation
-- Performs the join using nested loops
-- Do this only on reasonable sized tables! (O(n^2))
-- Query in SQL:
-- SELECT sum(tbl5_fact.col2), avg(tbl5_dim1.col1) FROM tbl5_fact,tbl5_dim1 WHERE tbl5_fact.col1=tbl5_dim1.col1 AND tbl5_fact.col2 < 30000 AND tbl5_dim1.col3<300;
--
--
p1=select(db1.tbl5_fact.col2,null, 30000)
p2=select(db1.tbl5_dim1.col3,null, 300)
f1=fetch(db1.tbl5_fact.col1,p1)
f2=fetch(db1.tbl5_dim1.col1,p2)
t1,t2=join(f1,p1,f2,p2,nested-loop)
col2joined=fetch(db1.tbl5_fact.col2,t1)
col1joined=fetch(db1.tbl5_dim1.col1,t2)
a1=sum(col2joined)
a2=avg(col1joined)
print(a1,a2)
