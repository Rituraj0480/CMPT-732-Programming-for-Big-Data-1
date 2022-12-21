import sys, os, gzip, re, uuid
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from datetime import datetime


def main(input_dir, keyspace, table_name):
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)

    batch = BatchStatement()
    count=0
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
                list_arr = line_re.split(line)
                if len(list_arr)==6:
                    batch.add(SimpleStatement("INSERT INTO " + table_name + " (id, host, datetime, path, bytes) VALUES (%s, %s, %s, %s, %s)"), (uuid.uuid4(), list_arr[1], datetime.strptime(list_arr[2],'%d/%b/%Y:%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'), list_arr[3], int(list_arr[4])))
                    count+=1
                if(count==200):
                    session.execute(batch)
                    count=0
                    batch.clear()
    session.execute(batch)    #for last remaining entries
    batch.clear()



if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_dir, keyspace, table_name)