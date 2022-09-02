-- Test for a non-clustered index select followed by an aggregate
--
-- Query form in SQL:
-- SELECT sum(col3) FROM tbl4_clustered_btree WHERE (col2 >= _ and col2 < _);
--
s0=select(db1.tbl4_clustered_btree.col2,8024,9024)
f0=fetch(db1.tbl4_clustered_btree.col3,s0)
a0=sum(f0)
print(a0)
s1=select(db1.tbl4_clustered_btree.col2,37549,38549)
f1=fetch(db1.tbl4_clustered_btree.col3,s1)
a1=sum(f1)
print(a1)
s2=select(db1.tbl4_clustered_btree.col2,49903,50903)
f2=fetch(db1.tbl4_clustered_btree.col3,s2)
a2=sum(f2)
print(a2)
s3=select(db1.tbl4_clustered_btree.col2,66218,67218)
f3=fetch(db1.tbl4_clustered_btree.col3,s3)
a3=sum(f3)
print(a3)
s4=select(db1.tbl4_clustered_btree.col2,110697,111697)
f4=fetch(db1.tbl4_clustered_btree.col3,s4)
a4=sum(f4)
print(a4)
