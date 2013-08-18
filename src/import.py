'''
Import sample csv data into Cassandra.

Created on Aug 18, 2013
@author: paleksandrov
'''
import cql

def main():
    path = '/Users/paleksandrov/Documents/SoundCloud/Cassandra/Data/favoritings.csv'
        
    con = None
    cursor = None
    try:
        con = cql.connect('localhost', 9160, 'fav_test', cql_version='3.0.0')
        print ("Connected!")
        cursor = con.cursor()
        
        # TODO (paleksandrov): Benchmark
        with open(path, 'rb') as csvfile:
            rownum = 0
            
            # Skip header
            next(csvfile)
            for line in csvfile:
                # print str(rownum) + ',' + line
                data = line.split(',', 1)
                    
                cql_string = "INSERT INTO favorites (user_id, song_id, rownum) VALUES ({0}, {1}, {2});"
                cql_string = cql_string.format(long(data[0]), long(data[1]), rownum)
                cursor.execute(cql_string)
            
                rownum += 1
                if (rownum >= 100) : break
            
            print "Successfully inserted {0} favs\n".format(rownum)
    finally:
        if cursor : cursor.close()
        if con : con.close()

if __name__ == '__main__':
    main()