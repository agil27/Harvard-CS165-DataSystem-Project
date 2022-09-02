-- Test for a clustered index select followed by a second predicate
--
-- Query in SQL:
-- SELECT sum(col1) FROM tbl4 WHERE (col2 >= 53522 and col2 < 53524);
-- SELECT sum(col1) FROM tbl4 WHERE (col2 >= 83866 and col2 < 83868);
--
s1=select(db1.tbl4.col2,53522,53524)
f1=fetch(db1.tbl4.col1,s1)
a1=sum(f1)
print(a1)
s2=select(db1.tbl4.col2,83866,83868)
f2=fetch(db1.tbl4.col1,s2)
a2=sum(f2)
print(a2)
