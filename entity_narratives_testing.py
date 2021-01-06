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

# ee = Es()
# client = ee.get_client("144.167.35.89")
# client.close()
# json_body = {
#     "entity": "Development Initiatives",
#     "data": [
#         {
#             "narrative": "a group called Development Initiatives based in Bristol.",
#             "blogpost_ids": [
#                 12
#             ]
#         }
#     ],
#     "last_modified_time": "2020-10-26 12:00:14"
# }
# ee.insert_record(client, self.index, 'Development Initiatives', 'doc', json_body)
# ee.delete_record(client, self.index, 'Development Initiatives', 'doc_type')


class EntityNarratives(SqlFuncs, Functions, Es):
    def __init__(self, connect, index):
        self.connect = connect
        self.index = index

    def get_records(self):
        connection = self.get_connection(self.connect)

        with connection.cursor() as cursor:
            # Getting last recorded element
            f = open('last_sql.txt', 'r')
            line = f.readline().split('--')
            f.close()
            last_elem = int(line[0]) - 1
            t = int(line[1])
            # last_elem = t - last_elem

            query = """
            SELECT narratives FROM narratives where narratives not like '{}' limit """ + str(last_elem) + """ , 1000000000000
            """
            # query = """
            # SELECT narratives FROM narratives where narratives not like '{}' limit 40700, 1000
            # """

            # query = """
            # SELECT narratives FROM narratives where narratives not like '{}'
            # """
            cursor.execute(query)
            narrative_record = cursor.fetchall()

        cursor.close()
        connection.close()
        return narrative_record

    def process_narratives(self, d):
        dddd = json.loads(d['narratives'])
        actions = []
        for entity in dddd:
            flag = False
            json_request = {
                "query": {
                    "term": {
                        "entity.keyword": {
                            "value": entity
                        }
                    }
                }
            }
            client = self.get_client("144.167.35.89")
            records = self.search_record(
                client, self.index, json_request)
            client.transport.close()

            if records['hits']['hits']:
                temp_json = records['hits']['hits'][0]['_source']['data']

            if len(records['hits']['hits']) == 1:
                if records['hits']['hits'][0]['_source']['entity'] != entity:
                    print('first')

            if len(records['hits']['hits']) > 1:
                print('second')

            if entity == 'China':
                pass
                # print('here')

            for x in dddd[entity]:
                narr = x['narrative']
                blogpost_id = x['blogpost_id']

                if not records['hits']['hits']:
                    # doc = [{"narrative": narr, "blogpost_ids": [blogpost_id]}]
                    json_body = {
                        "_index": self.index,
                        "_id": entity,
                        "_source": {
                            "entity": entity,
                            "data": [
                                {
                                    "narrative": narr,
                                    "blogpost_ids": [blogpost_id]
                                }
                            ]
                        }}

                    actions.append(json_body)
                    # if not self.insert_record(client, self.index, entity, 'doc', json_body):
                    #     # print('no insert')
                    #     pass
                else:
                    data = temp_json
                    narratives = [(i, x['narrative']) for i, x in enumerate(
                        data) if narr == x['narrative']]
                    blogpost_ids = [x['blogpost_ids']
                                    for x in data if narr == x['narrative']]

                    if narratives and blogpost_ids[0]:
                        connection = self.get_connection(self.connect)
                        with connection.cursor() as cursor:
                            cursor.execute(f"select blogpost_id from blogposts where blogpost_id in ({','.join(map(str,blogpost_ids[0]))})")
                            blogpost_ids = cursor.fetchall()
                            blogpost_ids = [x['blogpost_id'] for x in blogpost_ids]
                            flag = True
                            if blogpost_ids:
                                data[narratives[0][0]]['blogpost_ids'] = blogpost_ids
                            else:
                                del data[narratives[0][0]]
                        cursor.close()
                        connection.close()

                    if not narratives:
                        flag = True
                        doc = {"narrative": narr,
                               "blogpost_ids": [blogpost_id]}
                        data.append(doc)

                    elif blogpost_id not in blogpost_ids:
                        flag = True
                        narrative_index = narratives[0][0]
                        if blogpost_ids:
                            if blogpost_ids[0] == [] and len(blogpost_ids):
                                blogpost_ids = []
                        blogpost_ids.append(blogpost_id)
                        data[narrative_index]['blogpost_ids'] = blogpost_ids
                    elif not blogpost_ids:
                        print('here')

            if flag:
                json_body = {
                    "_index": self.index,
                    "_id": entity,
                    "_source": {
                        "entity": entity,
                        "data": data
                    }
                }

                actions.append(json_body)
                # if not self.update_record(client, self.index, entity, '_doc', json_body):
                #     # print('no update')
                #     pass

        if actions:
            client = self.get_client("144.167.35.89")
            bulk_action = self.bulk_request(client, actions)
            # bulk_action = helpers.bulk(client, actions)
            client.transport.close()
            if bulk_action[0] != len(actions):
                print('here')
            # bulk_parallel = helpers.parallel_bulk(client, actions)
        


connect = get_config()
if __name__ == "__main__":
    parallel = True
    sub_parallel = False

    EN = EntityNarratives(connect, 'entity_narratives_reindex')
    narrative_record = EN.get_records()
    f = open('last_sql.txt', 'r')
    start = f.readline().split('--')[0]
    f.close()

    if not parallel:
        pbar = tqdm(narrative_record, total=len(
            narrative_record), desc="Narratives")
        for record in pbar:
            EN.process_narratives(record)
            pbar.update()
            f = open('last_sql.txt', 'w')
            f.write(str(int(start) + int(pbar.last_print_n)) +
                    '--' + str(len(narrative_record)))
            f.close()

    else:
        # d = []
        # for x in narrative_record:
        #     d.append((x, sub_parallel))

        print("starting multi-process")
        # process_pool = ProcessPool(12)
        # pbar = tqdm(process_pool.imap(EN.process_narratives, narrative_record), desc="Narratives", ascii=True,  file=sys.stdout, total=len(narrative_record))
        # for x in pbar:
        #     pbar.update(1)

        # process_pool = Pool(int(12))
        # pbar = tqdm(process_pool.map(EN.process_narratives, narrative_record), desc="Narratives", ascii=True,  file=sys.stdout, total=len(narrative_record))
        # for x in pbar:
        #     pbar.update(1)
        f = open('last_sql.txt', 'r')
        start = f.readline().split('--')[0]
        f.close()

        with Pool(processes=16) as process_pool:
            with tqdm(total=len(narrative_record)) as pbar:
                for i, _ in enumerate(process_pool.imap_unordered(EN.process_narratives, narrative_record)):
                    pbar.update()
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

