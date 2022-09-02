-- Test Multiple Selects + Average
--
-- SELECT avg(col2) FROM tbl1 WHERE col1 >= 956 and col1 < 972;
s1=select(db1.tbl1.col1,956,972)
f1=fetch(db1.tbl1.col2,s1)
a1=avg(f1)
print(a1)
