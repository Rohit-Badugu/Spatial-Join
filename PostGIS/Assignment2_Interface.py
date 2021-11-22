#
# Assignment2 Interface
#

import psycopg2
import os, time
import sys, threading

# Do not close the connection inside this file i.e. do not perform openConnection.close()

def spatial_join(rectFragment, pointFragment, joinFragment, openConnection):
    cursor = openConnection.cursor()
    cursor.execute('ALTER TABLE ' + rectFragment + ' RENAME geom TO rectGeom')
    cursor.execute('ALTER TABLE ' + pointFragment + ' RENAME geom TO pointGeom')
    cursor.execute('CREATE TABLE ' + joinFragment + ' AS SELECT rectGeom, count(*) FROM '+ rectFragment + ',' + pointFragment + ' WHERE ST_Contains(' + rectFragment + '.rectGeom, '+ pointFragment + '.pointGeom) GROUP BY ' + rectFragment + '.rectGeom')
    openConnection.commit()

def createPointsPartition(pointsTable, openConnection):
	cursor = openConnection.cursor()
	#create fragments
	print('Creating tables')
	cursor.execute('CREATE TABLE p1 AS SELECT * FROM '+ pointsTable + ' WHERE longitude >= -73.95 AND latitude >= 40.75')
	cursor.execute('CREATE TABLE p2 AS SELECT * FROM '+ pointsTable + ' WHERE longitude < -73.95 AND latitude >= 40.75')
	cursor.execute('CREATE TABLE p3 AS SELECT * FROM '+ pointsTable + ' WHERE longitude >= -73.95 AND latitude < 40.75')
	cursor.execute('CREATE TABLE p4 AS SELECT * FROM '+ pointsTable + ' WHERE longitude < -73.95 AND latitude < 40.75')
	openConnection.commit()
	
def createRectsPartition(rectsTable, openConnection):
	cursor = openConnection.cursor()
	#create fragments
	print('Creating tables')

	cursor.execute('CREATE TABLE r1 AS SELECT * FROM '+ rectsTable + ' WHERE longitude1 >= -73.95 AND latitude1 >= 40.75')
	cursor.execute('CREATE TABLE r2 AS SELECT * FROM '+ rectsTable + ' WHERE longitude1 < -73.95 AND latitude1 >= 40.75')
	cursor.execute('CREATE TABLE r3 AS SELECT * FROM '+ rectsTable + ' WHERE longitude1 >= -73.95 AND latitude1 < 40.75')
	cursor.execute('CREATE TABLE r4 AS SELECT * FROM '+ rectsTable + ' WHERE longitude1 < -73.95 AND latitude1 < 40.75')
	openConnection.commit()

def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    #Implement ParallelJoin Here.
    createPointsPartition(pointsTable, openConnection)
    createRectsPartition(rectsTable, openConnection)
	
    #spatial_join('r1','p1','x1',openConnection)
    #Creating threads
    
    thread1 = threading.Thread(target=spatial_join, args=('r1', 'p1', 'x1', openConnection))
    thread2 = threading.Thread(target=spatial_join, args=('r2', 'p2', 'x2', openConnection))
    thread3 = threading.Thread(target=spatial_join, args=('r3', 'p3', 'x3', openConnection))
    thread4 = threading.Thread(target=spatial_join, args=('r4', 'p4', 'x4', openConnection))

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='Rohit69', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


