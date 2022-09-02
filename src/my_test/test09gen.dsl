-- Big Bad Boss Test! Milestone 1
-- It's basically just the previous tests put together
-- But also, its.... Boss test!

-- SELECT avg(col1+col2), min(col2), max(col3), avg(col3-col2), sum(col3-col2) FROM tbl2 WHERE (col1 >= -338888 AND col1 < -22660) AND (col2 >= -249339 AND col2 < 66889);
s1=select(db1.tbl2.col1,-338888,-22660)
sf1=fetch(db1.tbl2.col2,s1)
s2=select(s1,sf1,-249339,66889)
f1=fetch(db1.tbl2.col1,s2)
f2=fetch(db1.tbl2.col2,s2)
f3=fetch(db1.tbl2.col3,s2)
add12=add(f1,f2)
out1=avg(add12)
out2=min(f2)
out3=max(f3)
sub32=sub(f3,f2)
out4=avg(sub32)
out5=sum(sub32)
print(out1,out2,out3,out4,out5)
