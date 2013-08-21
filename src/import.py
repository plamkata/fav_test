'''
Import sample csv data into Cassandra.

Created on Aug 18, 2013
@author: paleksandrov
'''
import cql
import time

def data_stream(row_limit_num=0):
    path = '/Users/paleksandrov/Documents/SoundCloud/Cassandra/Data/favoritings.csv'
    
    with open(path, 'rb') as csvfile:
        row_num = 0
        
        # Skip header
        next(csvfile)
        for line in csvfile:
            # print str(row_num) + ',' + line
            data = line.split(',', 1)
            
            yield [data, row_num]

            row_num += 1
            if (row_limit_num != 0 and row_num >= row_limit_num) : break
                
def batch_stream(batch_size=1000):
    batch_strings = []
    row_num = 0
    
    for row in data_stream():
        data = row[0]
        row_num = row[1]
        
        cql_string = "INSERT INTO favorites (user_id, song_id) VALUES ({0}, {1})\n"
        cql_string = cql_string.format(long(data[0]), long(data[1]))
        batch_strings.append(cql_string)
        
        if ((row_num + 1) % batch_size == 0) : 
            yield [''.join(batch_strings), len(batch_strings), row_num]
            batch_strings = []
            
    if (batch_strings) :
        yield [''.join(batch_strings), len(batch_strings), row_num];
        batch_strings = []
        
def connect_cql():
    con = None
    cursor = None
    try:
        con = cql.connect('localhost', 9160, 'fav_test', cql_version='3.0.0')
        print ("Connected!")
        cursor = con.cursor()
        
        yield cursor;
    
    except Exception, e:
        print "Error occurred: " + str(e)
    finally:
        if cursor : cursor.close()
        if con : con.close()

def main():
    for cursor in connect_cql() :
        # Benchmark
        begin = time.clock()
        
        row_num = 0
        batch_num = 0
        success_count = 0
        
        for batch in batch_stream():
            try:
                cql_string = "BEGIN BATCH\n" + batch[0] + "APPLY BATCH;"
                cursor.execute(cql_string)
                
                success_count += batch[1]
                
            except Exception, e:
                print "Problem with batch {0} ignored: {1}". \
                    format(batch_num, str(e))
            finally:
                row_num = batch[2];
                batch_num += 1
                
                # report progress on 1000 batches
                if (batch_num % 1000 == 0) :
                    print "Processed {0} batches and {1} favs with {2} failures in {3} sec.". \
                        format(batch_num, row_num + 1, row_num + 1 - success_count, 
                               time.clock() - begin)
            
        end = time.clock()
            
        print "Successfully inserted {0} favs with {1} failures in {2} sec.\n". \
            format(row_num + 1, row_num + 1 - success_count, end - begin)

if __name__ == '__main__':
    main()
