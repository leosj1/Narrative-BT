from sql import SqlFuncs
from functions import pos_tag_narratives, run_comprehensive, entity_narratives, get_config
from functions_class import Functions

import json
from tqdm import tqdm
from pathos.multiprocessing import freeze_support, ProcessPool
import sys
from multiprocessing import Process, Pool, RLock
import time
from elasticsearch import helpers

from ElasticsearchIndices.es import Es
import asyncio
import aiomysql


class EntityNarratives(SqlFuncs, Functions, Es):
    def __init__(self, connect, index):
        self.connect = connect
        self.index = index
        self.actions = []

    def get_records(self):
        connection = self.get_connection(self.connect)

        with connection.cursor() as cursor:
            # Getting last recorded element
            f = open('last_sql.txt', 'r')
            line = f.readline().split('--')
            f.close()
            last_elem = int(line[0]) - 1
            t = int(line[1])

            query = """
            SELECT n.narratives, b.date, b.blogsite_id 
            FROM narratives n , blogposts b 
            where n.narratives not like '{}' 
            and b.blogpost_id = n.blogpost_id 
            limit """ + str(last_elem) + """, 100000000000
            """
            cursor.execute(query)
            narrative_record = cursor.fetchall()

        cursor.close()
        connection.close()
        return narrative_record

    def process_narrative_elastic(self, d):
        dddd = json.loads(d['narratives'])
        actions = []

        for entity in dddd:
            for x in dddd[entity]:
                narr = x['narrative']
                blogpost_id = x['blogpost_id']
                json_request = {
                    "size": 10000,
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "entity": {
                                            "value": entity
                                        }
                                    }
                                },
                                {
                                    "term": {
                                        "blogpost_id": {
                                            "value": blogpost_id
                                        }
                                    }
                                },
                                {
                                    "term": {
                                        "narrative": {
                                            "value": narr
                                        }
                                    }
                                }
                            ]
                        }

                    }
                }
                client = self.get_client("144.167.35.89")
                records = self.search_record(client, self.index, json_request)
                client.transport.close()

                if not records['hits']['hits']:
                    json_body = {
                        "_index": self.index,
                        "_source": {
                            "blogpost_id": blogpost_id,
                            "blogsite_id":d['blogsite_id'],
                            "narrative": narr,
                            "entity": entity,
                            "date":d['date'],
                            "narrative_keyword":narr
                        }
                    }
                    self.actions.append(json_body)

        return self.actions

    def process_narratives(self, d):
        dddd = json.loads(d['narratives'])

        for entity in dddd:
            connection = self.get_connection(self.connect)
            with connection.cursor() as cursor:
                for x in dddd[entity]:
                    narr = x['narrative']
                    blogpost_id = x['blogpost_id']

                    query = f"""SELECT * 
                                FROM blogtrackers.{self.index}
                                where entity = '{entity}'
                                AND blogpost_id = {blogpost_id}
                                AND narrative = '{narr}'"""
                    cursor.execute(query)
                    records = cursor.fetchall()

                    if not records:
                        self.update_insert('''INSERT INTO ''' + self.index + ''' (entity, blogpost_id, narrative) values (%s, %s, %s) ''', (entity, blogpost_id, narr), connect)
                        
            cursor.close()
            connection.close()


connect = get_config()
if __name__ == "__main__":
    parallel = True
    sub_parallel = False

    # EN = EntityNarratives(connect, 'entity_narrative_testing')
    EN = EntityNarratives(connect, 'entity_narratives_map_all_reindex')
    narrative_record = EN.get_records()
    f = open('last_sql.txt', 'r')
    start = f.readline().split('--')[0]
    f.close()
    actions = []

    if not parallel:
        pbar = tqdm(narrative_record, total=len(narrative_record), desc="Narratives")
        for record in pbar:
            d = EN.process_narrative_elastic(record)
            actions+=d

            client = EN.get_client("144.167.35.89")
            bulk_action = EN.bulk_request(client, d)
            client.transport.close()
            if bulk_action[0] != len(d):
                print('here')

            f = open('last_sql.txt', 'w')
            f.write(str(int(start) + int(pbar.last_print_n)) +
                    '--' + str(len(narrative_record)))
            f.close()

    else:
        print("starting multi-process")
        f = open('last_sql.txt', 'r')
        start = f.readline().split('--')[0]
        f.close()

        actions = []

        with Pool(processes=16) as process_pool:
            with tqdm(total=len(narrative_record)) as pbar:
                for i, d in enumerate(process_pool.imap_unordered(EN.process_narrative_elastic, narrative_record)):
                    pbar.update()
                    actions+=d

                    client = EN.get_client("144.167.35.89")
                    bulk_action = EN.bulk_request(client, d)
                    client.transport.close()
                    if bulk_action[0] != len(d):
                        print('here')

                    f = open('last_sql.txt', 'w')
                    f.write(str(int(start) + int(pbar.last_print_n)) +
                            '--' + str(len(narrative_record)))
                    f.close()

        print("Finished processing!")

        print("\nClosing pool")
        process_pool.close()
        print("Joining pool")
        process_pool.join()
        print("Clearing pool")
        # process_pool.clear()
        print("Finished!")
