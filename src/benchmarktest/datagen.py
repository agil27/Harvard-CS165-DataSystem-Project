import pandas as pd
import numpy as np

def gen_milestone1():
    # test milestone 1
    ## test 1 number of rows, fix selectivity
    for n in [100, 1000, 10000, 100000, 1000000]:
        numbers = np.arange(100).tolist() * (n // 100)
        # print(numbers)
        np.random.shuffle(numbers)
        df = {
            'db1.tbl1.col1': np.arange(n),
            'db1.tbl1.col2': numbers
        }
        df = pd.DataFrame(df)
        # df.reset_index('db1.tbl1.col1')
        df.to_csv('./benchmarktest/m1_%d.csv' % n, index=None)
        with open('./benchmarktest/m1_%d.dsl' % n, 'w') as f:
            f.write('''create(db,"db1")\n\
create(tbl,"tbl1",db1,2)\n\
create(col,"col1",db1.tbl1)\n\
create(col,"col2",db1.tbl1)\n\
load("benchmarktest/m1_%d.csv")\n\
s1=select(db1.tbl1.col2,null,20)\n\
f1=fetch(db1.tbl1.col2,s1)\n\
f2=avg(f1)\n\
print(f2)\n\
shutdown\n''' % n)

    ## test selectivity
    numbers = np.arange(100000).tolist() * 10
    np.random.shuffle(numbers)
    df = {
        'db1.tbl1.col1': np.arange(n),
        'db1.tbl1.col2': numbers
    }
    df = pd.DataFrame(df)
    # df.reset_index('db1.tbl1.col1')
    df.to_csv('./benchmarktest/m1_sel.csv', index=None)

    for s in [1, 0.1, 0.01, 0.001, 0.0001, 0.00001]:
        with open('./benchmarktest/m1_sel_%d.dsl' % (200000 * s), 'w') as f:
            f.write('''create(db,"db1")\n\
create(tbl,"tbl1",db1,2)\n\
create(col,"col1",db1.tbl1)\n\
create(col,"col2",db1.tbl1)\n\
load("benchmarktest/m1_sel_%d.csv")\n\
s1=select(db1.tbl1.col2,null,%d)\n\
f1=fetch(db1.tbl1.col2,s1)\n\
f2=avg(f1)\n\
print(f2)\n\
shutdown\n''' % (20000 * s, 20000 * s))


def gen_milestone2():
    # test milestone 2
    ## test number of batched queries
    for n in range(1, 21):
        with open('./benchmarktest/m2_%d.dsl' % n, 'w') as f:
            command = '''create(db,"db1")\n\
create(tbl,"tbl1",db1,2)\n\
create(col,"col1",db1.tbl1)\n\
create(col,"col2",db1.tbl1)\n\
load("benchmarktest/m1_sel_20000.csv")\n\
batch_queries()\n'''
            for i in range(n):
                command += 's%d=select(db1.tbl1.col2,null,1000)\n' % i
            command += 'batch_execute()\nshutdown\n'
            f.write(command)


def gen_milestone3():
    # test milestone 3
    ## test number of rows
    for n in [100, 1000, 10000, 100000, 1000000]:
        col1 = np.arange(n).tolist()
        col2 = np.arange(100).tolist() * (n // 100)
        np.random.shuffle(col2)
        df = {
            'db1.tbl1.col1': col1,
            'db1.tbl1.col2': col2,
            'db1.tbl1.col3': col2,
            'db1.tbl1.col4': col2
        }
        df = pd.DataFrame(df)
        df.to_csv('./benchmarktest/m3_%d.csv' % n, index=None)

        #load
        with open('./benchmarktest/m3_%d_load.dsl' % n, 'w') as f:
            f.write('''create(db,"db1")\n\
create(tbl,"tbl1",db1,4)\n\
create(col,"col1",db1.tbl1)\n\
create(col,"col2",db1.tbl1)\n\
create(col,"col3",db1.tbl1)\n\
create(col,"col4",db1.tbl1)\n\
create(idx,db1.tbl1.col3,sorted,unclustered)\n\
create(idx,db1.tbl1.col4,btree,unclustered)\n\
load("benchmarktest/m3_%d.csv")\n\
shutdown\n''' % n)

        # scan
        with open('./benchmarktest/m3_%d_scan.dsl' % n, 'w') as f:
            f.write('s1=select(db1.tbl1.col2,null,20)\nshutdown\n')

        # binsearch
        with open('./benchmarktest/m3_%d_binsearch.dsl' % n, 'w') as f:
            f.write('s2=select(db1.tbl1.col3,null,20)\nshutdown\n')

        # btree
        with open('./benchmarktest/m3_%d_btree.dsl' % n, 'w') as f:
            f.write('s3=select(db1.tbl1.col4,null,20)\nshutdown\n')

    # test selectivity
    col1 = np.arange(1000000)
    col2 = np.arange(1000000)
    np.random.shuffle(col2)
    df = {
        'db1.tbl1.col1': col1,
        'db1.tbl1.col2': col2,
        'db1.tbl1.col3': col2,
        'db1.tbl1.col4': col2
    }
    df = pd.DataFrame(df)
    df.to_csv('./benchmarktest/m3_sel.csv', index=None)

    # load
    with open('./benchmarktest/m3_sel_load.dsl', 'w') as f:
        f.write('''create(db,"db1")\n\
create(tbl,"tbl1",db1,4)\n\
create(col,"col1",db1.tbl1)\n\
create(col,"col2",db1.tbl1)\n\
create(col,"col3",db1.tbl1)\n\
create(col,"col4",db1.tbl1)\n\
create(idx,db1.tbl1.col3,sorted,unclustered)\n\
create(idx,db1.tbl1.col4,btree,unclustered)\n\
load("benchmarktest/m3_sel.csv")\n\
shutdown\n''')

    for s in [200000, 20000, 2000, 200, 20, 2]:
        # scan
        with open('./benchmarktest/m3_sel_%d_btree.dsl' % s, 'w') as f:
            f.write('s1=select(db1.tbl1.col2,null,%d)\nshutdown\n' % (s, ))

        # binsearch
        with open('./benchmarktest/m3_sel_%d_binsearch.dsl' % s, 'w') as f:
            f.write('s2=select(db1.tbl1.col3,null,%d)\nshutdown\n' % (s, ))

        # btree
        with open('./benchmarktest/m3_sel_%d_btree.dsl' % s, 'w') as f:
            f.write('s3=select(db1.tbl1.col4,null,%d)\nshutdown\n' % (s, ))


def gen_milestone4():
    # test number of rows
    for n in [100, 1000, 10000, 100000, 1000000]:
        col1 = np.arange(n).tolist()
        col2 = np.arange(n // 2, n // 2 + n).tolist()
        np.random.shuffle(col1)
        np.random.shuffle(col2)
        df = {
            'db1.tbl1.col1': col1,
            'db1.tbl1.col2': col2,
        }
        df = pd.DataFrame(df)
        # df.reset_index('db1.tbl1.col1')
        df.to_csv('./benchmarktest/m4_%d.csv' % n, index=None)

        # load
        with open('./benchmarktest/m4_load_%d.dsl' % n, 'w') as f:
            f.write('''create(db,"db1")\n\
create(tbl,"tbl1",db1,2)\n\
create(col,"col1",db1.tbl1)\n\
create(col,"col2",db1.tbl1)\n\
load("benchmarktest/m4_%d.csv")\n\
shutdown\n''' % (n,))

        # nested-loop
        with open('./benchmarktest/m4_loop_%d.dsl' % n, 'w') as f:
            f.write('''s1=select(db1.tbl1.col1,null,null)\n\
s2=select(db1.tbl1.col2,null,null)\n\
f1=fetch(db1.tbl1.col1,s1)
f2=fetch(db1.tbl2.col2,s2)
t1,t2=join(f1,s1,f2,s2,nested-loop)
shutdown\n''')

        # hash
        with open('./benchmarktest/m4_hash_%d.dsl' % n, 'w') as f:
            f.write('''s1=select(db1.tbl1.col1,null,null)\n\
s2=select(db1.tbl1.col2,null,null)\n\
f1=fetch(db1.tbl1.col1,s1)
f2=fetch(db1.tbl2.col2,s2)
t1,t2=join(f1,s1,f2,s2,hash)
shutdown\n''')

    # test selectivity
    for s in [50, 500, 5000, 50000, 500000]:
        col1 = np.arange(1000000).tolist()
        col2 = np.arange(s, s + n).tolist()
        np.random.shuffle(col1)
        np.random.shuffle(col2)
        df = {
            'db1.tbl1.col1': col1,
            'db1.tbl1.col2': col2,
        }
        df = pd.DataFrame(df)
        # df.reset_index('db1.tbl1.col1')
        df.to_csv('./benchmarktest/m4_sel_%d.csv' % s, index=None)

        # load
        with open('./benchmarktest/m4_sel_load_%d.dsl' % s, 'w') as f:
            f.write('''create(db,"db1")\n\
create(tbl,"tbl1",db1,2)\n\
create(col,"col1",db1.tbl1)\n\
create(col,"col2",db1.tbl1)\n\
load("benchmarktest/m4_sel_%d.csv")\n\
shutdown\n''' % (s, ))

        # nested-loop
        with open('./benchmarktest/m4_sel_loop_%d.dsl' % s, 'w') as f:
            f.write('''s1=select(db1.tbl1.col1,null,%d)\n\
s2=select(db1.tbl1.col2,null,%d)\n\
f1=fetch(db1.tbl1.col1,s1)
f2=fetch(db1.tbl2.col2,s2)
t1,t2=join(f1,s1,f2,s2,nested-loop)
shutdown\n''' % (1000000 * 0.2, 1000000 * 0.2 + s))

        # hash
        with open('./benchmarktest/m4_sel_hash_%d.dsl' % s, 'w') as f:
            f.write('''s1=select(db1.tbl1.col1,null,%d)\n\
s2=select(db1.tbl1.col2,null,%d)\n\
f1=fetch(db1.tbl1.col1,s1)
f2=fetch(db1.tbl2.col2,s2)
t1,t2=join(f1,s1,f2,s2,hash)
shutdown\n''' % (1000000 * 0.2, 1000000 * 0.2 + s))


# gen_milestone1()
# gen_milestone2()
# gen_milestone3()
gen_milestone4()
