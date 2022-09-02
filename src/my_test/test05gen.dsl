-- Summation
--
-- SELECT SUM(col3) FROM tbl2 WHERE col1 >= -395877 AND col1 < 404123;
s1=select(db1.tbl2.col1,-395877,404123)
f1=fetch(db1.tbl2.col3,s1)
a1=sum(f1)
print(a1)
--
-- SELECT SUM(col1) FROM tbl2;
a2=sum(db1.tbl2.col1)
print(a2)
