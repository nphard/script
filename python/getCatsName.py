#!/usr/bin/env python
"""
This script is used to get the categories tree relationship 
Author: Laisheng
Version: 1.0
"""

import sys
import getopt
import codecs
import bsddb

def usage():
    print " getCatsName.py --datalist files --output out.txt  [--verbose]\n"


def createHash(hashfile,datalist,verbose):
    hashdb = bsddb.hashopen(hashfile,"c")
    flist = open(datalist,"r")
    for f in flist:
        f=f.strip()
        if f != "":
            if(verbose):
                print "Start to processs the file : %s" % (f)
        
            datafile = codecs.open(f,"r","UTF-8")
            count = 0
            for line in datafile:
                record=line.strip()
                if record != "":
                    values=record.split("\t")
                    if len(values) > 3:
                        value= values[1]+"\t"+values[3]
                        hashdb[values[0].encode("UTF-8")]= value.encode("UTF-8")
                    else:
                        errmsg="Error line value: %s " % line
                        print errmsg.encode("UTF-8")
                count += 1
                if (verbose and count % 100000==0):
                    print "Have processed %d lines" % (count)
            datafile.close()
    flist.close()
    return hashdb
    

def main():
    try:
        opts,args=getopt.getopt(sys.argv[1:],
                                "hd:v",
                                ["help",
                                 "verbose",
                                 "datalist=",
                                 "output="])
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    datalist = None
    verbose = False
    output = None
    for o, a in opts:
        if o in ("-v","--verbose"):
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-o","--output"):
            output = open(a,"wb") 
        elif o in ("-d","--datalist"):
            datalist = a 
        else:
            assert False, "unhandled option"

    if(verbose):
        print "datalist=%s" % ( datalist)
    
    if not (datalist and output) :
        usage()
        sys.exit()

    print "Start to initialize the id/name database"
    hashdb=createHash("cathash.db",datalist,verbose)
    hashdb=bsddb.hashopen("cathash.db","r")
    print "Success initialize the id/name hash database\n"

    hashsize = len(hashdb)
    
    (key,value) = hashdb.first()
    for i in xrange(1,hashsize):
        path = ""
        name = None
        pid  = None

        (pid,name)=value.split("\t")
        path = name

        while pid != "0":
            if hashdb.has_key(pid):
                (pid,name) = hashdb[pid].split("\t")
                path =  name + "\t" + path
            else:
                print "Can't find parent id in hash %s" % (pid)
                path =  pid + "\t" + path
                parentid="0"
                break

        output.write(path+"\n")
        (key,value)=hashdb.next()

    output.close()
    hashdb.close()

if __name__ == "__main__":
    main()

    
    
