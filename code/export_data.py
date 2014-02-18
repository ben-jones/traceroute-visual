#!/usr/bin/env python

import dbhash
import sys
import fnmatch
import cPickle as cp
import pg as pgsql
import time

def sqlconn():
    try:
    #print "try"
        conn = pgsql.connect(dbname='swati',user='',passwd='')
        print "Connection Established"
    except:
        print "Could not connect to sql server"
        sys.exit()
    return conn

def run_insert_query(conn, key, value, day):
    if conn== None:
        conn = sqlconn
    print "Command Construction"
    for val in value:
        test_time = time.strftime('%Y-%m-%d',time.gmtime(int(key[1])))	
        if test_time == day:
            return False
        time_val = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(int(key[1])))	
        try:
            cmd  = "INSERT into traceroute(deviceid,eventstamp,srcip,dstip,toolid,hop,ip,rtt)values('%s','%s','%s','%s','%s','%s','%s','%s')"%(key[0],time_val,key[2],key[3],key[4],val[0],val[1],val[2]);
            #print cmd
            conn.query(cmd)
        except:
            print "Couldn't run %s\n"%(cmd)
            continue
    return

def export_day(day):
    db = dbhash.open("/data/users/swati/traceroutes--2013-10.db", "r")
    conn = sqlconn()

    for keys in db.keys():
        key = cp.loads(keys)
        value = cp.loads(db[keys])
        retVal = run_insert_query(conn, key, value, day)
        # if there is an error or we have gone past the correct day,
        # then break out
        if retVal == False:
            break

if __name__ == "__main__":
    export_day('2014-02-01')
    conn.close()
    db.close()
