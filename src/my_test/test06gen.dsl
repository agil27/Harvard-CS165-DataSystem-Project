-- Addition
--
-- SELECT col2+col3 FROM tbl2 WHERE col1 >= -466796 AND col1 < -466776;
s11=select(db1.tbl2.col1,-466796,-466776)
f11=fetch(db1.tbl2.col2,s11)
f12=fetch(db1.tbl2.col3,s11)
a11=add(f11,f12)
print(a11)
