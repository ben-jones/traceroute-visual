#!/usr/bin/env python

import dbhash
import sys
import fnmatch
import cPickle as cp
import sqlite3
import time

def run_insert_query(conn, key, value, day):
    print "Command Construction"
    for val in value:
        test_time = time.strftime('%Y-%m-%d',time.gmtime(int(key[1])))	
        if test_time == day:
            return False
        time_val = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(int(key[1])))	
#        try:
        cmd  = "INSERT into traceroute(deviceid,eventstamp,srcip,dstip,toolid,hop,ip,rtt)values('%s','%s','%s','%s','%s','%s','%s','%s')"%(key[0],time_val,key[2],key[3],key[4],val[0],val[1],val[2]);
            #print cmd
        conn.execute(cmd)
#        except:
#            print "Couldn't run %s\n"%(cmd)
#            continue
    return

def export_day(day, conn):
    db = dbhash.open("traceroutes--2014-01.db", "r")

    print len(db.keys())
#    for keys in db.keys():
#        key = cp.loads(keys)
#        value = cp.loads(db[keys])
#        retVal = run_insert_query(conn, key, value, day)
#        # if there is an error or we have gone past the correct day,
#        # then break out
#        if retVal == False:
#            break

#    conn.commit()

def create_table(conn):
    create_table_query = ("CREATE table traceroute (deviceid text, eventstamp text, "
                          "srcip text, dstip text, toolid text, hop integer, ip text, rtt real)")
    conn.execute(create_table_query)
    conn.commit()

if __name__ == "__main__":
    conn = sqlite3.connect('traceroute-data.db')
#    create_table(conn)
    export_day('2014-01-01', conn)
    conn.close()
    db.close()
