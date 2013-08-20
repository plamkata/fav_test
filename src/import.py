'''
Import sample csv data into Cassandra.

Created on Aug 18, 2013
@author: paleksandrov
'''
import cql
import time

def data_stream(row_limit_num=100000):
    path = '/Users/paleksandrov/Documents/SoundCloud/Cassandra/Data/favoritings.csv'
    
    with open(path, 'rb') as csvfile:
        row_num = 0
        
        # Skip header
        next(csvfile)
        for line in csvfile:
            # print str(row_num) + ',' + line
            data = line.split(',', 1)
            
            try:
                yield [data, row_num]
            except Exception, e:
                print str(e)
            finally:
                row_num += 1
                if (row_limit_num != 0 and row_num >= row_limit_num) : break
                
def batch_stream(batch_size=1000):
    batch_strings = []
    
    for chunk in data_stream():
        data = chunk[0]
        row_num = chunk[1]
        
        cql_string = "INSERT INTO favorites (user_id, song_id) VALUES ({0}, {1})\n"
        cql_string = cql_string.format(long(data[0]), long(data[1]))
        batch_strings.append(cql_string)
        
        if ((row_num + 1) % batch_size == 0) : 
            yield [''.join(batch_strings), len(batch_strings), row_num]
            batch_strings = []
            
    if (batch_strings) :
        yield ''.join(batch_strings);

def main():
    con = None
    cursor = None
    try:
        con = cql.connect('localhost', 9160, 'fav_test', cql_version='3.0.0')
        print ("Connected!")
        cursor = con.cursor()
        
        # TODO (paleksandrov): Benchmark
        begin = time.clock()
        row_num = 0
        success_count = 0
        
        for batch in batch_stream():
            cql_string = "BEGIN BATCH\n" + batch[0] + "APPLY BATCH;"
            cursor.execute(cql_string)
            row_num = batch[2];
            
            success_count += batch[1]
            
        end = time.clock()
            
        print "Successfully inserted {0} favs with {1} failures in {2} sec.\n". \
            format(row_num + 1, row_num + 1 - success_count, end - begin)
    
    except Exception, e:
        print "Error occurred: " + str(e)
    finally:
        if cursor : cursor.close()
        if con : con.close()

if __name__ == '__main__':
    main()
