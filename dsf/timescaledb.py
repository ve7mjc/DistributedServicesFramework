#
# 
#
from dsf.component import Component
from dsf.messageadapters import MessageOutputAdapter

import psycopg2
from queue import Queue,Empty

dbuser="msc_writer"
dbpass = "v8R7Hm5$ku6CBY3p"
dbname = "mdas"

CONNECTION = "postgres://msc_writer:v8R7Hm5$ku6CBY3p@localhost/mdas" # :5432

import threading

class Writer(MessageOutputAdapter,Component):
    
    _adapter_type = "TimescaleDbWriter"
    _message_direction = "out"
    
    message_queue = Queue()
    
    def __init__(self,**kwargs):
        
        #self.lock = threading.RLock()
        #self.condition = threading.Condition()

        self._start_required = True
        self._stop_required = True
        
        super().__init__(**kwargs)

    def start(self):
        super().start()
        self.connect()
            
    def stop(self,reason=None):
        super().stop(reason)
        self.pg_conn.close()
        
    def connect(self):
        try:
            self.pg_conn = psycopg2.connect(CONNECTION)
            self.log_info("connected to postgresql")
        except:
            self.log_exception()

#    def run(self):
# 
#        while self.keep_running:
#            
#            if self.condition.wait(timeout=0.05) == False:
#                continue
#
#            self.log_debug("releasing from condition.wait()")
#
#            try:
#                message = self.message_queue.get_nowait()
#                print(message)
##                with self.pg_conn:
##                    with self.pg_conn.cursor() as curs:
##                        curs.execute(SQL1)
#                        
#            except Empty:
#                pass
#
#        self.log_info("shutting down psycopg2 adapter!")
#
#        
#        self.pg_conn.close()
        
            
            #SQL = "INSERT INTO authors (name) VALUES (%s);" # Note: no quotes
            #data = ("O'Reilly", )
            #cur.execute(SQL, data) # Note: no % operator
        
#        insert_data(conn)
#        cur = conn.cursor()

#        query_create_sensors_table = "CREATE TABLE sensors (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50));"
#        cur.execute(query_create_sensors_table)        
#        conn.commit()
#        cur.close()

    #    Here’s a typical pattern you’d use to insert some data into a table. In the
    #    example below, we insert the relational data in the array sensors, into the
    #    relational table named sensors.
    #
    #    First, we open a cursor with our connection to the database, then using prepared
    #    statements formulate our INSERT SQL statement and then we execute that
    #    statement,

    # Write Message to Adapter
    def write(self, message):
        self.message_queue.put(message)
        try:
            with self.pg_conn:
                with self.pg_conn.cursor() as curs:
                    SQL = "INSERT INTO weather_swob_testing (test) VALUES (%s);" # Note: no quotes
                    data = ("testing", )
                    curs.execute(SQL, data) # Note: no % operator
        except Exception as e:
            #self.log_exception()
            self.log_error(e.__repr__())
        
        #                with self.pg_conn:
#                    with self.pg_conn.cursor() as curs:
#                        curs.execute(SQL1)
        
        
#    Python builtin datetime, date, time, timedelta are converted into PostgreSQL’s timestamp[tz], date, time[tz], interval data types. Time zones are supported too. The Egenix mx.DateTime objects are adapted the same way:
#
#    >>> dt = datetime.datetime.now()
#    >>> dt
#    datetime.datetime(2010, 2, 8, 1, 40, 27, 425337)
#
#    >>> cur.mogrify("SELECT %s, %s, %s;", (dt, dt.date(), dt.time()))
#    "SELECT '2010-02-08T01:40:27.425337', '2010-02-08', '01:40:27.425337';"
#
#    >>> cur.mogrify("SELECT %s;", (dt - datetime.datetime(2010,1,1),))
#    "SELECT '38 days 6027.425337 seconds';"

