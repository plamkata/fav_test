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
        CQLString = "INSERT INTO favourites (user_id, song_id, rownum) VALUES (131, 81, 15);"
        cursor.execute(CQLString)
    finally:
        if cursor : cursor.close()
        if con : con.close()
    
    with open(path, 'rb') as csvfile:
        rownum = 0
        
        # Skip header
        next(csvfile)
        for line in csvfile:
            print str(rownum) + ',' + line
            
            rownum += 1
            if (rownum >= 100) : break

if __name__ == '__main__':
    main()