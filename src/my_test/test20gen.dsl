--
-- Query in SQL:
-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= 192874 and col3 < 193074;
-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= 45030 and col3 < 45430;
--
s1=select(db1.tbl4_ctrl.col3,192874,193074)
f1=fetch(db1.tbl4_ctrl.col1,s1)
print(f1)
s2=select(db1.tbl4_ctrl.col3,45030,45430)
f2=fetch(db1.tbl4_ctrl.col1,s2)
print(f2)
