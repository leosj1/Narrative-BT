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
# import mysql.connector
import multiprocessing
from multiprocessing import Process, Pool
from pathos.multiprocessing import freeze_support, ProcessPool
import sys
from tqdm import tqdm
import collections
import functools
import operator
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

    # main function
    def main(self):
        connection = self.get_connection(self.connect)
        with connection.cursor() as cursor:
            # Getting already populated blogpost_id from tracker_narratives table
            # blogpost_ids_query = f"""
            #                         SELECT JSON_KEYS(blogpost_narratives) AS blogpost_id
            #                         from tracker_narratives
            #                         where tid = {self.tid}
            #                     """
            # cursor.execute(blogpost_ids_query)
            # results = cursor.fetchall()
            # if results:
            #     blogpost_ids = results[0]['blogpost_id']
            #     if blogpost_ids != '[]':
            #         query1 = f"""
            #                 SELECT a.blogpost_id, a.narratives, a.entity_count
            #                 FROM (
            #                     SELECT blogpost_id, narratives, entity_count
            #                     FROM narratives
            #                     where {self.blog_ids}
            #                     ) a
            #                 where a.blogpost_id not in {blogpost_ids.replace('[','(').replace(']',')')}
            #             """
            #     else:
            #         query1 = f"""
            #                     SELECT blogpost_id, narratives, entity_count
            #                     FROM narratives
            #                     where {self.blog_ids}
            #             """
            # else:
            #     query1 = f"""
            #                 SELECT blogpost_id, narratives, entity_count
            #                 FROM narratives
            #                 where {self.blog_ids}
            #         """

            query1 = f"""
                            SELECT blogpost_id, narratives, entity_count 
                            FROM narratives 
                            where {self.blog_ids}
                            limit 10
                    """

            # Getting narratives from narratives table

            # self.blog_ids = self.blog_ids.replace('blogsite_id in', 'b.blogsite_id in')
            # query1 = f"""SELECT n.blogpost_id blogpost_id, n.narratives narratives, n.entity_count entity_count FROM blogposts b left join narratives n on b.blogpost_id = n.blogpost_id where {self.blog_ids} and b.blogpost_id not in {blogpost_ids.replace('[','(').replace(']',')')}"""

            cursor.execute(query1)
            records = cursor.fetchall()

        connection.close()
        cursor.close()

        self.process_main(records)
        self.process_entity_count()

        # p2 = Process(target=self.process_main, args=(records, ))
        # p1 = Process(target=self.process_entity_count)
        # p2.start()
        # p1.start()
        # p2.join()
        # p1.join()

    # process data in parallel or single process

    def process_main(self, records):
        if not self.parallel:
            for record in tqdm(records, total=len(records), desc=f"Narratives - TID_{self.tid}"):
                self.process_data(record)
        else:
            print("starting multi-process")
            with Pool(int(self.num_processes)) as p:
                pbar = tqdm(p.imap_unordered(self.process_data, records),
                            desc=f"Narratives - TID_{self.tid}", ascii=True,  file=sys.stdout, total=len(records))
                for _ in pbar:
                    pbar.update(1)

            # process_pool = ProcessPool(self.num_processes)
            # pbar = tqdm(process_pool.imap(self.process_data, records), desc="Narratives", ascii=True,  file=sys.stdout, total=len(records))
            # for x in pbar:
            #     pbar.update(1)

            # for record in tqdm(process_pool.imap(self.process_data, records), desc=f"Narratives - TID_{self.tid}", ascii=True,  file=sys.stdout, total=len(records)):
            #     pass

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
            data = (self.tid, '{}', json.dumps(top_entities), self.blog_ids)
            self.update_insert(
                "insert into tracker_narratives (tid, blogpost_narratives, top_entities, query) values (%s, %s, %s, %s)", data, connect)

        print('top_entity done')

    # Process, insert or update narratives for tracker

    def process_data(self, record):
        connection = self.get_connection(connect)
        with connection.cursor() as cursor:
            narratives_query = f"""select blogpost_narratives, top_entities from tracker_narratives where tid = {self.tid}"""
            cursor.execute(narratives_query)
            narratives_record = cursor.fetchall()

            blogpostID = record['blogpost_id']
            entity_count = record['entity_count']
            data_narratives = record['narratives']

            blogpost_narratives_file = {}
            temp_narratives_checked = {}
            temp_narratives_checked[str(blogpostID)] = ast.literal_eval(data_narratives)
            blogpost_narratives_file['blogpost_narratives'] = temp_narratives_checked

            with open(f'json_files/tid_{self.tid}.json', 'w') as json_file:
                json.dump(blogpost_narratives_file, json_file)

            # if narratives_record:
                # blogpost_entities_checked = json.loads(narratives_record[0]['blogpost_entities']) if narratives_record[0]['blogpost_entities'] else {}
                # blogpost_entities_checked[str(blogpostID)] = json.loads(entity_count)

                # blogpost_entities_checked = json.dumps(blogpost_entities_checked)
                # bp_narratives_checked = json.loads(narratives_record[0]['blogpost_narratives']) if narratives_record[0]['blogpost_narratives'] else {}
                # bp_narratives_checked[str(blogpostID)] = json.loads(data_narratives)
                # bp_narratives_checked = json.dumps(bp_narratives_checked)

                # data = blogpost_entities_checked, bp_narratives_checked, self.blog_ids, self.tid

                # f = open(f'json_files/tid_{self.tid}.json', 'rb')
                # data = json.load(f)
                # temp_dict = data['blogpost_narratives_file']
                # data = json.dumps(record['blogpost_narratives']), self.blog_ids, self.tid
                # self.update_insert("update tracker_narratives set blogpost_narratives = %s, query = %s where tid = %s", data, connect)
            # else:
                # temp_blogpost_entities = {}
                # temp_blogpost_entities[str(blogpostID)] = json.loads(entity_count)
                # temp_blogpost_entities = json.dumps(temp_blogpost_entities)

                # temp_narratives_checked = {}
                # temp_narratives_checked[str(blogpostID)] = ast.literal_eval(data_narratives)
                # temp_narratives_checked = json.dumps(temp_narratives_checked)

                # # terms_sorted = json.dumps({k: int(v) for k, v in sorted(ast.literal_eval(entity_count).items(), key=lambda item: item[1], reverse = True) if k != '.'})
                # data = self.tid, temp_blogpost_entities, temp_narratives_checked, self.blog_ids
                # data = self.tid, json.dumps(record['blogpost_narratives']), self.blog_ids
                # self.update_insert("insert into tracker_narratives (tid, blogpost_narratives, query) values (%s, %s, %s)", data, connect)



        connection.close()
        cursor.close()


if __name__ == "__main__":
    import time
    from datetime import datetime
    start = time.time()

    s = SqlFuncs(connect)
    connection = s.get_connection(connect)
    tracker_blog_id = {}

    with connection.cursor() as cursor:
        # Tracker ids
        trackers = "select tid from trackers"
        # trackers = """
        #     select t.tid
        #     from trackers t
        #     left join tracker_narratives tn
        #     on t.tid = tn.tid
        #     where t.tid is null
        #     or tn.tid is null
        #     or tn.top_entities = "{}"
        #     or tn.top_entities is null
        #     or tn.blogpost_narratives like '{}'
        #     or t.tid in (7, 11);
        # """
        cursor.execute(trackers)
        records = cursor.fetchall()

        # Getting tracker details and blog ids
        for x in records:
            tid = x['tid']
            # tid = 48
            tracker_details = f"""SELECT query from trackers where tid = {tid}"""
            cursor.execute(tracker_details)
            records_tracker = cursor.fetchall()
            blog_ids = records_tracker[0]['query']

            tracker_blog_id[tid] = blog_ids

    connection.close()
    cursor.close()

    def runn(tid, blog_ids, parallel, num_processes, connect):
        narr = Narratives(tid, blog_ids, parallel, num_processes, connect)
        narr.main()

    # Run for all trackers
    process_list = []
    for tid in tracker_blog_id:
        parallel = False
        num_processes = multiprocessing.cpu_count() * 2
        blog_ids = tracker_blog_id[tid]

        runn(tid, blog_ids, parallel, num_processes, connect)

        # p1 = Process(target=runn, args=(tid, blog_ids, parallel, num_processes, connect, ))
        # p1.start()
        # process_list.append(p1)

    # for p in process_list:
    #     p.join()

        end = time.time()
        runtime_mins, runtime_secs = divmod(end - start, 60)
        runtime_mins = round(runtime_mins, 0)
        runtime_secs = round(runtime_secs, 0)
        print("Time to complete: {} Mins {} Secs".format(runtime_mins,
                                                         runtime_secs), f'AT - {datetime.today().isoformat()}')
