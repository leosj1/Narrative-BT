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
import mysql.connector
from multiprocessing import Process, Pool
from pathos.multiprocessing import freeze_support, ProcessPool
import sys
from tqdm import tqdm
import collections, functools, operator 
from builtins import dict
from sql import SqlFuncs
# from functions import pos_tag_narratives, run_comprehensive, entity_narratives
from functions_class import Functions

# Database connection 
connect = 'cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers'

class Narratives(SqlFuncs, Functions):
    def __init__(self, tid, blog_ids, parallel, num_processes, connect):
        self.tid = tid
        self.blog_ids = blog_ids
        self.parallel = parallel
        self.num_processes = num_processes
        self.connect = connect
    
    def main(self):
        connection = self.get_connection(self.connect)
        with connection.cursor() as cursor:
            # Getting narratives from narratives table
            blogpost_ids_query = f"""
                                    SELECT JSON_KEYS(blogpost_narratives) AS blogpost_id 
                                    from tracker_narratives 
                                    where tid = {self.tid}
                                """
            cursor.execute(blogpost_ids_query)
            blogpost_ids = cursor.fetchall()[0]['blogpost_id']

            # self.blog_ids = self.blog_ids.replace('blogsite_id in', 'b.blogsite_id in')
            # query1 = f"""SELECT n.blogpost_id blogpost_id, n.narratives narratives, n.entity_count entity_count FROM blogposts b left join narratives n on b.blogpost_id = n.blogpost_id where {self.blog_ids} and b.blogpost_id not in {blogpost_ids.replace('[','(').replace(']',')')}"""
            query1 = f"""
                        SELECT a.blogpost_id, a.narratives, a.entity_count
                        FROM (
                            SELECT blogpost_id, narratives, entity_count 
                            FROM narratives 
                            where {self.blog_ids}
                            ) a
                        where a.blogpost_id not in {blogpost_ids.replace('[','(').replace(']',')')}
                    """
            cursor.execute(query1)
            records = cursor.fetchall()
            
        connection.close()
        cursor.close() 

        self.process_main(records)
        self.process_entity_count(records)

        # p1 = Process(target=self.process_main, args=(records, ))
        # p2 = Process(target=self.process_entity_count, args=(records, ))
        # p1.start()
        # p2.start()
        # p1.join()
        # p2.join()
                

        


    def process_main(self, records):
        if not self.parallel:
            for record in tqdm(records, total=len(records), desc="Narratives"):
                self.process_data(record)            
        else:
            print("starting multi-process")
            with Pool(int(self.num_processes)) as p:
                pbar = tqdm(p.imap_unordered(self.process_data, records), desc="Narratives", ascii=True,  file=sys.stdout, total=len(records))
                for _ in pbar:
                    pbar.update(1)

    def process_entity_count(self, records):
        entity_counts = [ast.literal_eval(x['entity_count']) for x in records if x['entity_count']]
        x_ = dict(functools.reduce(operator.add, map(collections.Counter, entity_counts)))
        terms_sorted = json.dumps({k: int(v) for k, v in sorted(x_.items(), key=lambda item: item[1], reverse = True) if k != '.'})
        data = terms_sorted, self.tid
        self.update_insert("update tracker_narratives set top_entities = %s where tid = %s", data, connect)
        # connection = self.get_connection(connect)
        # with connection.cursor() as cursor:
            # narratives_query = f"""select blogpost_entities, blogpost_narratives, top_entities from tracker_narratives where tid = {self.tid}"""
            # cursor.execute(narratives_query)
            # narratives_record = cursor.fetchall()
            # if not narratives_record:



    def process_data(self, record):
        connection = self.get_connection(connect)
        with connection.cursor() as cursor:
            narratives_query = f"""select blogpost_entities, blogpost_narratives, top_entities from tracker_narratives where tid = {self.tid}"""
            cursor.execute(narratives_query)
            narratives_record = cursor.fetchall()

            blogpostID =  record['blogpost_id']
            entity_count = record['entity_count']
            data_narratives = record['narratives']
            
            if narratives_record:
                blogpost_entities_checked = json.loads(narratives_record[0]['blogpost_entities']) if 'blogpost_entities' in narratives_record[0] else None
                blogpost_entities_checked[str(blogpostID)] = json.loads(entity_count)

                # x_ = dict(functools.reduce(operator.add, map(collections.Counter, blogpost_entities_checked.values())))

                # terms_sorted = json.dumps({k: int(v) for k, v in sorted(x_.items(), key=lambda item: item[1], reverse = True) if k != '.'})  
                # terms_sorted = json.dumps(x_)

                # terms_sorted = json.dumps("{}")

                blogpost_entities_checked = json.dumps(blogpost_entities_checked)
                bp_narratives_checked = json.loads(narratives_record[0]['blogpost_narratives']) if 'blogpost_narratives' in narratives_record[0] else None     
                bp_narratives_checked[str(blogpostID)] = json.loads(data_narratives)
                bp_narratives_checked = json.dumps(bp_narratives_checked)

                # top_entity_checked = ast.literal_eval(narratives_record[0]['top_entities']) if 'top_entities' in narratives_record[0] else None
                # x_ = dict(functools.reduce(operator.add, map(collections.Counter, top_entity_checked.values()))) 
                # terms_sorted = json.dumps({k: int(v) for k, v in sorted(x_.items(), key=lambda item: item[1], reverse = True) if k != '.'})     

                data = blogpost_entities_checked, bp_narratives_checked, self.blog_ids, self.tid
                self.update_insert("update tracker_narratives set blogpost_entities = %s, blogpost_narratives = %s, query = %s where tid = %s", data, connect)
            else:
                temp_blogpost_entities = {}
                temp_blogpost_entities[str(blogpostID)] = ast.literal_eval(entity_count)
                temp_blogpost_entities = json.dumps(temp_blogpost_entities)

                temp_narratives_checked = {}
                temp_narratives_checked[str(blogpostID)] = ast.literal_eval(data_narratives)
                temp_narratives_checked = json.dumps(temp_narratives_checked)

                terms_sorted = json.dumps({k: int(v) for k, v in sorted(ast.literal_eval(entity_count).items(), key=lambda item: item[1], reverse = True) if k != '.'})
                data = self.tid, temp_blogpost_entities, temp_narratives_checked, terms_sorted, self.blog_ids
                self.update_insert("insert into tracker_narratives (tid, blogpost_entities, blogpost_narratives, top_entities, query) values (%s, %s, %s, %s, %s)", data, connect)
            
        connection.close()
        cursor.close() 

                            
if __name__ == "__main__":
    import time
    from datetime import datetime
    start = time.time()

    s = SqlFuncs(connect)
    connection = s.get_connection(connect)
    with connection.cursor() as cursor: 
        # Getting tracker details and blog ids
        tid = 428 
        tracker_details = f"""SELECT query from trackers where tid = {tid}"""
        cursor.execute(tracker_details)
        records_tracker = cursor.fetchall()
        blog_ids = records_tracker[0]['query']

        parallel = False
        num_processes = 50
    
    connection.close()
    cursor.close()

    narr = Narratives(tid, blog_ids, parallel, num_processes, connect)
    narr.main()

    end = time.time()
    runtime_mins, runtime_secs = divmod(end - start, 60)
    runtime_mins = round(runtime_mins, 0)
    runtime_secs = round(runtime_secs, 0)
    print("Time to complete: {} Mins {} Secs".format(runtime_mins, runtime_secs), f'AT - {datetime.today().isoformat()}')