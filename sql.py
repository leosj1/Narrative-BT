import pymysql
import json
import time
import os
import asyncio
import aiomysql
from tqdm import tqdm

class SqlFuncs(): 
    def __init__(self, conn):
        self.conn = conn

    def run(self, debug=False):
        asyncio.run(self._run_all(), debug=debug)

    async def _run_all(self):
        self.pool = await aiomysql.create_pool(host=self.host, port=3306,
                                      user=self.user, password=self.password, db=self.db, maxsize=20)
        [await f for f in tqdm(asyncio.as_completed(self.tasks),
            total=len(self.tasks), desc="SQL")]
        self.pool.close()
        await self.pool.wait_closed()


    async def exectue(self, query, data):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if data: 
                        await cur.execute(query, data)
                        await conn.commit()
                    else: await cur.execute(query)
                    records = await cur.fetchall()
            conn.close()
            return records
        except (AttributeError, pymysql.err.OperationalError):
            #asyncio.exceptions.IncompleteReadError: X bytes read on a total of Y expected bytes
            # print("\nFailed to recieve all the data from the db. Re-running the query as blocking.")
            return self.block_execute(query, data)

    def block_execute(self, query, data):
        host, user, password, db = self.connect
        connection = pymysql.connect(host=host, user=user, 
            password=password, db=db, cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            if data: 
                cursor.execute(query, data)
                connection.commit()
            else: cursor.execute(query)
            records = cursor.fetchall()
        connection.close()
        return records

    def get_connection(self, conn):
        count = 0
        while True:
            count += 1
            try:
                host, user, password, db = conn
                connection = pymysql.connect(host=host,user=user,password=password,db=db,
                                    charset='utf8mb4',
                                    use_unicode=True,
                                    cursorclass=pymysql.cursors.DictCursor)
                return connection
            #Error handeling
            except Exception as e:
                if isinstance(e, pymysql.err.OperationalError): 
                    # Unable to access port (Windows Error), trying again
                    # See https://docs.microsoft.com/en-us/biztalk/technical-guides/settings-that-can-be-modified-to-improve-network-performance
                    # print("Socket error uploading to db. Trying again... {}".format(count))
                    time.sleep(3)
                    count += 1
                    if count > 10: print("Failed to connect to db {} times in a row".format(count))
                else: 
                    # Uncaught errors
                    raise Exception("We aren't catching this mySql get_connection Error: {}".format(e))


    def commit_to_db(self, query, data, db_dame):
        # while True: 
        try:
            connection = self.get_connection(db_dame)
            with connection.cursor() as cursor:
                cursor.execute(query, data)
                connection.commit()
                connection.close()
                return            
        #Error handeling
        except Exception as e:
            if isinstance(e, pymysql.err.IntegrityError) and e.args[0]==1062:
                # Duplicate Entry, already in DB
                print(e)
                connection.close() 
                return
            elif e.args[0] == 1406:
                # Data too long for column
                print(e)
                connection.close()
                return 
            else: 
                # Uncaught errors
                raise Exception("We aren't catching this mySql commit_to_db Error: {}".format(e))

    def update_insert(self, query, data, conn):
        connection = self.get_connection(conn)
        with connection.cursor() as cursor:
            try:
                cursor.execute(query, data)
                connection.commit()
            except Exception as e:
                if 'Duplicate entry' in str(e):
                    connection.close()
                    return str(e)
                else:
                    connection.close()
                    return ''
            
        connection.close()
        return ''
