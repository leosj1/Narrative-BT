# -*- coding: utf-8 -*-
'''
Created on Mar 28, 2020
@author: kxbandeli

Modified on Aug 1, 2020
@author: oljohnson
'''

# Import required libraries
from nltk import tokenize
import re
import ast
import json
import multiprocessing
from multiprocessing import Process, Pool, RLock
from pathos.multiprocessing import freeze_support, ProcessPool
import sys
from tqdm import tqdm
import collections
import functools
import operator
from builtins import dict
from sql import SqlFuncs
from functions_class import Functions
import os

# Database connection
connect = Functions().get_config()
# connect_mover = '144.167.35.89', 'db_mover', 'Cosmos1', 'blogtrackers'
file_lock = RLock()


def runn(PARAMS):
    from nltk import tokenize
    import re
    import ast
    import json
    # import mysql.connector
    import multiprocessing
    from multiprocessing import Process, Pool, RLock
    from pathos.multiprocessing import freeze_support, ProcessPool
    import sys
    from tqdm import tqdm
    import collections
    import functools
    import operator
    from builtins import dict
    from sql import SqlFuncs
    from functions_class import Functions
    import os

    # connect = 'cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers'
    connect = Functions().get_config()
    file_lock = RLock()

    class Narratives(SqlFuncs, Functions):
        def __init__(self, tid, blog_ids, parallel, num_processes, connect):
            self.tid = tid
            self.blog_ids = blog_ids
            self.parallel = parallel
            self.num_processes = num_processes
            self.connect = connect

        # main function
        def main(self):
            connection = self.get_connection(self.connect)
            with connection.cursor() as cursor:

                query1 = f"""
                                SELECT blogpost_id, narratives, entity_count 
                                FROM narratives 
                                where {self.blog_ids}
                                
                        """

                cursor.execute(query1)
                records = cursor.fetchall()

            connection.close()
            cursor.close()

            file_name = f'json_files/tid_{self.tid}.json'

            # Get ids already populated
            error = False
            if os.path.exists(file_name):
                f = open(file_name, 'r')
                try:
                    data_found = json.load(f)
                    keys = list(data_found.keys())
                    records = [x for x in records if str(x['blogpost_id']) not in keys]
                    
                except:
                    error = True
                    f.close()
                    os.remove(file_name)
            
            if not error:
                # Updating the DB from the json file
                self.process_main(records)

                if records:
                    with open(file_name, 'rb+') as f:
                        f.seek(-3, os.SEEK_END)
                        f.truncate()
                        f.write('}'.encode('ascii'))

                if os.path.exists(file_name):
                    f = open(file_name, 'r')
                    data = json.load(f)

                    if os.path.exists(file_name):
                        connection = self.get_connection(self.connect)
                        with connection.cursor() as cursor:
                            narratives_query = f"""select blogpost_narratives, top_entities from tracker_narratives where tid = {self.tid}"""
                            cursor.execute(narratives_query)
                            narratives_record = cursor.fetchall()
                        connection.close()
                        cursor.close()

                        
                        if narratives_record:
                            d = json.dumps(data), self.blog_ids, self.tid
                            self.update_insert(
                                "update tracker_narratives set blogpost_narratives = %s, query = %s where tid = %s", d, connect)
                        else:
                            d = self.tid, json.dumps(data), self.blog_ids
                            self.update_insert(
                                "insert into tracker_narratives (tid, blogpost_narratives, query) values (%s, %s, %s)", d, connect)
                    

                        # Getting top entities for tracker
                        self.process_entity_count()
            else:
                self.main()

        # process data in parallel or single process
        def process_main(self, records):
            if not self.parallel:
                # for record in records:
                for record in tqdm(records, total=len(records), desc=f"Narratives - TID_{self.tid}"):
                    self.process_data(record)
            else:
                print("starting multi-process")
                with Pool(int(self.num_processes)) as p:
                    pbar = tqdm(p.map(self.process_data, records),
                                desc=f"Narratives - TID_{self.tid}", ascii=True,  file=sys.stdout, total=len(records))
                    for _ in pbar:
                        pbar.update(1)

                print("Finished processing!")
                print("\nClosing pool")
                p.close()
                print("Joining pool")
                p.join()
                print("Clearing pool")
                # p.clear()
                print("Finished!")

        # Getting top terms from entity_count field for selected narratives
        def process_entity_count(self):
            connection = self.get_connection(connect)
            with connection.cursor() as cursor:
                entity_query = f"""
                            select n.term, sum(n.occurr) occurrence
                            from narratives, 
                            json_table(entity_count,
                            '$[*]' columns(
                                term varchar(128) path '$.term',
                                occurr int(11) path '$.occurrence'
                                )
                            ) 
                            as n
                            where {self.blog_ids}
                            group by n.term
                            order by occurrence desc
                            limit 100
                        """
                cursor.execute(entity_query)
                entity_record = cursor.fetchall()

                narratives_query = f"""select blogpost_narratives, top_entities from tracker_narratives where tid = {self.tid}"""
                cursor.execute(narratives_query)
                narratives_record = cursor.fetchall()

            connection.close()
            cursor.close()

            top_entities = {}
            for entity in entity_record:
                term = entity['term']
                occurrence = entity['occurrence']
                top_entities[term] = int(occurrence)

            if narratives_record:
                data = json.dumps(top_entities), self.tid
                self.update_insert(
                    "update tracker_narratives set top_entities = %s where tid = %s", data, connect)
            else:
                data = (self.tid, '{}', json.dumps(
                    top_entities), self.blog_ids)
                self.update_insert(
                    "insert into tracker_narratives (tid, blogpost_narratives, top_entities, query) values (%s, %s, %s, %s)", data, connect)

            # print('top_entity done')

        # Process, insert or update narratives for tracker
        def process_data(self, record):
            connection = self.get_connection(connect)
            with connection.cursor() as cursor:
                blogpostID = record['blogpost_id']
                data_narratives = record['narratives']

                # error = False
                file_name = f'json_files/tid_{self.tid}.json'
                if not os.path.exists(file_name):
                    with open(file_name, 'w') as f:
                        f.write('{}')

                result = [ast.literal_eval(data_narratives)]
                with file_lock:
                    with open(file_name, 'rb+') as f:
                        f.seek(-1, os.SEEK_END)
                        f.truncate()
                        for entry in result:
                            _entry = '"{}":{},\n'.format(
                                str(blogpostID), json.dumps(entry))
                            _entry = _entry.encode()
                            f.write(_entry)
                        f.write('}'.encode('ascii'))
            connection.close()
            cursor.close()

    tid, blog_ids, parallel, num_processes, connect = PARAMS
    narr = Narratives(tid, blog_ids, parallel, num_processes, connect)
    narr.main()


if __name__ == "__main__":
    import time
    from datetime import datetime
    

    s = SqlFuncs(connect)
    connection = s.get_connection(connect)

    # s_mover = SqlFuncs(connect_mover)
    # connection_mover = s_mover.get_connection(connect_mover)

    tracker_blog_id = {}

    with connection.cursor() as cursor:
        # Tracker ids
        # trackers = "select tid from trackers"
        trackers = """
            select t.tid
            from trackers t
            left join tracker_narratives tn
            on t.tid = tn.tid
            where t.tid is null
            or tn.tid is null
            or tn.top_entities = "{}"
            or tn.top_entities is null
            or tn.blogpost_narratives like '{}';
        """
        cursor.execute(trackers)
        records = cursor.fetchall()

        # Getting tracker details and blog ids
        for x in records:
            tid = x['tid']
            # tid = 450
            tracker_details = f"""SELECT query from trackers where tid = {tid}"""
            cursor.execute(tracker_details)
            records_tracker = cursor.fetchall()
            blog_ids = records_tracker[0]['query']
            if blog_ids != 'blogsite_id in ()' and 'NaN' not in blog_ids:
                tracker_blog_id[tid] = blog_ids

    connection.close()
    cursor.close()

    DATA = []
    for tid in tracker_blog_id:
        parallel = False
        num_processes = 8
        # num_processes = multiprocessing.cpu_count() * 2
        blog_ids = tracker_blog_id[tid]

        PARAMS = tid, blog_ids, parallel, num_processes, connect
        DATA.append(PARAMS)

    # Run for all trackers
    parallel_main = True
    num_processes_main = 12

    if parallel_main:
        start = time.time()

        process_pool = ProcessPool(num_processes_main)
        pbar = tqdm(process_pool.imap(runn, DATA), desc="Narratives",ascii=True,  file=sys.stdout, total=len(records))
        for x in pbar:
            pbar.update(1)

        print("Finished processing!")

        print("\nClosing pool")
        process_pool.close()
        print("Joining pool")
        process_pool.join()
        print("Clearing pool")
        process_pool.clear()
        print("Finished!")

        end = time.time()
        runtime_mins, runtime_secs = divmod(end - start, 60)
        runtime_mins = round(runtime_mins, 0)
        runtime_secs = round(runtime_secs, 0)
        print("Time to complete: {} Mins {} Secs".format(runtime_mins,runtime_secs), f'AT - {datetime.today().isoformat()}')
    else:
        process_list = []
        for x in DATA:
            runn(x)
