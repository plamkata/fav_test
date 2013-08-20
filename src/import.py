'''
Import sample csv data into Cassandra.

Created on Aug 18, 2013
@author: paleksandrov
'''
import cql
import time

def data_stream(limit_num=100000):
    path = '/Users/paleksandrov/Documents/SoundCloud/Cassandra/Data/favoritings.csv'
    
    with open(path, 'rb') as csvfile:
        rownum = 0
        fails = 0
        
        # Skip header
        next(csvfile)
        for line in csvfile:
            # print str(rownum) + ',' + line
            data = line.split(',', 1)
            
            try:
                yield [data, rownum, fails]
            except:
                fails += 1
            finally:
                rownum += 1
                if (limit_num != 0 and rownum >= limit_num) : break

def main():
    con = None
    cursor = None
    try:
        con = cql.connect('localhost', 9160, 'fav_test', cql_version='3.0.0')
        print ("Connected!")
        cursor = con.cursor()
        
        # TODO (paleksandrov): Benchmark
        begin = time.clock()
        rownum = 0
        fails = 0
        for chunk in data_stream() :
            data = chunk[0]
            rownum = chunk[1]
            fails = chunk[2]
            
            cql_string = "INSERT INTO favorites1 (user_id, song_id, rownum) VALUES ({0}, {1}, {2});"
            cql_string = cql_string.format(long(data[0]), long(data[1]), rownum)
            cursor.execute(cql_string)
            
        end = time.clock()
            
        print "Successfully inserted {0} favs with {1} failures in {2} sec.\n". \
            format(rownum + 1, fails, end - begin)
    finally:
        if cursor : cursor.close()
        if con : con.close()

if __name__ == '__main__':
    main()
