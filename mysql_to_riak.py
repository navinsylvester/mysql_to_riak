#!/usr/bin/python

import MySQLdb as mdb
import sys
import riak

con = mdb.connect('localhost', 'root', 'passwd', 'db_name')
client = riak.RiakClient(port=8091)
bucket = client.bucket('bucket_name')

with con:
    cur = con.cursor(mdb.cursors.DictCursor)
    
    #Insert your query here
    cur.execute("SELECT t1.a as a, unix_timestamp(t1.b) as b, t1.c as c, t1.d as d, t1.e as e, t1.f as f, t1.g as g, t1.h as h, cast(t1.i as char(12)) as i, t2.j as j FROM table_one t1 INNER JOIN table_two t2 ON t1.a=t2.a WHERE t1.k='l'")

    rows = cur.fetchall()

    count = 0
    
    for row in rows:
        #Key name should be properly constructed
        #Key filter friendly
        #Helps to effectively do mapreduce
        b = row['b']
        a = row['a']
        j = row['j']

        key_name = str(b)+'-'+str(a)+'-'+str(j)
        
        print key_name
        print row
        
        count = count + 1
        print count

        create_kv = bucket.new(key_name, data=row)
        create_kv.store()

    cur.close()