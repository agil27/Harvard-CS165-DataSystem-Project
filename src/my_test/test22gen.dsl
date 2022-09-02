-- Test for a clustered index select followed by a second predicate
--
-- Query in SQL:
-- SELECT sum(col1) FROM tbl4 WHERE (col3 >= 70463 and col3 < 170463) AND (col2 >= 1132 and col2 < 3132);
--
s1=select(db1.tbl4.col3,70463,170463)
f1=fetch(db1.tbl4.col2,s1)
s2=select(s1,f1,1132,3132)
f2=fetch(db1.tbl4.col1,s2)
print(f2)
a1=sum(f2)
print(a1)
