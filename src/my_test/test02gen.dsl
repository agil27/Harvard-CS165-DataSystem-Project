-- Test Select + Fetch
--
-- SELECT col1 FROM tbl1 WHERE col1 < 20;
s1=select(db1.tbl1.col1,null,20)
f1=fetch(db1.tbl1.col1,s1)
print(f1)
--
-- SELECT col2 FROM tbl1 WHERE col1 >= 987;
s2=select(db1.tbl1.col1,987,null)
f2=fetch(db1.tbl1.col2,s2)
print(f2)
