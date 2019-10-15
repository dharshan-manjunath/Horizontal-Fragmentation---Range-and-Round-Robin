#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='123', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):

    cursor = openconnection.cursor()
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor.execute("DROP TABLE IF EXISTS " + ratingstablename);
    cursor.execute("CREATE TABLE IF NOT EXISTS " + ratingstablename + "(Row_ID SERIAL, UserID INT, MovieID INT, Rating real)")
    f = open(ratingsfilepath, "r")
    fileline = f.readlines()
    for line in fileline:
        fields = line.split("::")
        insert_query = "INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES({0}, {1}, {2})".format(fields[0], fields[1],
                                                                                           fields[2])
        cursor.execute(insert_query)
    f.close()
    openconnection.commit()
    cursor.close()
    pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):

    
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = openconnection.cursor()

    cursor.execute("""SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' AND LOWER(table_name) LIKE '"""+"""range_part"""+"""%';""");
    rows = cursor.fetchall();
    print(numberofpartitions);
    for row in rows:
##        print "dropping table: ", row[1]
        cursor.execute("DROP TABLE IF EXISTS "+ row[1] + " cascade")
    cursor.execute("DROP TABLE IF EXISTS metadata");
    
    cursor.execute("CREATE TABLE metadata(ID INT, lowerbound real, upperbound real)")
    partition = 5.0 / numberofpartitions
    lowerbound = 0
    for x in range(0, numberofpartitions):
        cursor.execute("CREATE TABLE IF NOT EXISTS range_part%s(UserID INT, MovieID INT, Rating real)" % x)
        lowerbound = lowerbound
        upperbound = lowerbound + partition
        cursor.execute(
            "INSERT INTO metadata(ID, lowerbound, upperbound)VALUES ({0}, {1}, {2})".format(x, lowerbound, upperbound))
        if lowerbound == 0:
            cursor.execute("INSERT INTO range_part" + str(x) + "(userid, movieid, rating) (SELECT userid, movieid, rating from " + str(ratingstablename) + " where rating >= " + str(lowerbound) + " and rating <= " + str(upperbound) + ")")
        else:
            cursor.execute(
                "INSERT INTO range_part" + str(x) + "(userid, movieid, rating) (SELECT userid, movieid, rating from " + str(ratingstablename) + " where rating >" + str(lowerbound) + " and rating <= " + str(upperbound) + ")")
        lowerbound = upperbound
    cursor.close()
    openconnection.commit()
    pass

    
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = openconnection.cursor()
    cursor.execute("""SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' AND LOWER(table_name) LIKE '"""+"""rrobin_part"""+"""%';""");
    rows = cur.fetchall();
##    print(rows);
    for row in rows:
##        print "dropping table: ", row[1]
        cursor.execute("DROP TABLE IF EXISTS " + row[1] + " cascade")
    cursor.execute("DROP TABLE IF EXISTS metadata_rr");
    cursor.execute("""CREATE TABLE metadata_rr (
                PartitionNumber smallint,
                NumberofPartitions smallint
                )""");
    cursor.execute("INSERT INTO metadata_rr (PartitionNumber,NumberofPartitions) VALUES(0,%s)",(str(numberofpartitions)));
    for i in range(numberofpartitions):
        cursor.execute("CREATE TABLE rrobin_part"+str(i)+ " AS SELECT * FROM "+ratingstablename+" WHERE row_id % "
                    + str(numberofpartitions) + " = " + str((i+1)%numberofpartitions));
        cursor.execute("update metadata_rr set partitionnumber = (SELECT MAX(row_id) from ratings)%"+str(numberofpartitions));


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = openconnection.cursor()
    cursor.execute("""SELECT PartitionNumber FROM metadata_rr""");
    if(bool(cursor.rowcount)==True):
##        print("in here");
        rows = cursor.fetchall();
        partitionnumber = int(rows[0][0]);
        cursor.execute(
                "INSERT INTO "+ratingstablename+"(userid,movieid,rating) VALUES (%s, %s, %s)",
                (userid, itemid, rating)
            )
        cursor.execute(
                "INSERT INTO rrobin_part"+str(partitionnumber)+"(userid,movieid,rating) VALUES (%s, %s, %s)",
                (userid, itemid, rating)
            )
        cursor.execute("update metadata_rr set partitionnumber = (partitionnumber+1)%NumberofPartitions");


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    cursor = openconnection.cursor()
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor.execute("INSERT INTO "+str(ratingstablename)+"(userid, movieid, rating) VALUES ({0}, {1}, {2})".format(userid, itemid, rating))
    cursor.execute("SELECT id FROM metadata where " + str(rating) + ">= lowerbound AND " + str(rating) + "<= upperbound")
    X = cursor.fetchone()[0]
    cursor.execute("INSERT INTO range_part" + str(X) + "(userid, movieid, rating) VALUES ({0}, {1}, {2})".format(userid, itemid,rating))

    cursor.close()
    openconnection.commit()
    pass

def deletepartitionsandexit(openconnection):

    cursor = openconnection.cursor()
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cursor.execute("SELECT COUNT(*) FROM metadata")
    totalrangepartitions= cursor.fetchone()[0]
    cursor.execute("SELECT * FROM metadata_rr")
    totalrrobinpartitions= cursor.fetchone()[0]

    for x in range(0, totalrrobinpartitions):
        cursor.execute("DROP TABLE rrobin_part%s"%x)
    for x in range(0, totalrangepartitions):
        cursor.execute("DROP TABLE range_part%s"%x)
    cursor.execute("DROP TABLE metadata")
    cursor.execute("DROP TABLE metadata_rr")
    cursor.close()
    openconnection.commit()
    pass

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()
