from sql import SqlFuncs
from functions import pos_tag_narratives, run_comprehensive, entity_narratives, get_config
from functions_class import Functions

import json
from tqdm import tqdm
from pathos.multiprocessing import freeze_support, ProcessPool
import sys
from multiprocessing import Process, Pool, RLock

class EntityNarratives(SqlFuncs, Functions):
    def __init__(self, connect):
        self.connect = connect

    def get_records(self):
        connection = self.get_connection(self.connect)

        with connection.cursor() as cursor:
            query = """
            SELECT narratives FROM narratives where narratives not like '{}' limit 40700, 10000000000
            """
            # query = """
            # SELECT narratives FROM narratives where narratives not like '{}' limit 16904, 1000
            # """
            cursor.execute(query)
            narrative_record = cursor.fetchall()

        cursor.close()
        connection.close()
        return narrative_record

    def process_narratives(self, d):
        dddd = json.loads(d['narratives'])
    
        for entity in dddd:
            # PARAMS = entity, dddd
            # entity, dddd = PARAMS
            connection = self.get_connection(self.connect)
            with connection.cursor() as cursor:
                query = f"""SELECT data FROM blogtrackers.entity_narratives where entity = '{entity}'"""
                cursor.execute(query)
                records = cursor.fetchall()

                for x in dddd[entity]:
                    narr = x['narrative']
                    blogpost_id = x['blogpost_id']
                    
                    if not records:
                        doc = [{"narrative": narr, "blogpost_ids": [blogpost_id]}]
                        self.update_insert('''INSERT INTO entity_narratives (entity, data) values (%s, %s) ''', (entity, json.dumps(doc)), connect)
                    else:
                        data = json.loads(records[0]['data'])
                        narratives = [(i, x['narrative']) for i,x in enumerate(data) if narr == x['narrative']]
                        blogpost_ids = [x['blogpost_ids'][0] for x in data if narr == x['narrative']]

                        if not narratives:
                            doc = {"narrative": narr, "blogpost_ids": [blogpost_id]}
                            data.append(doc)
                            self.update_insert('''UPDATE entity_narratives SET data = %s WHERE entity = %s''', (json.dumps(data), entity), connect)
                        elif blogpost_id not in blogpost_ids:
                            narrative_index = narratives[0][0]
                            data[narrative_index]['blogpost_ids'].append(blogpost_id)
                            self.update_insert('''UPDATE entity_narratives SET data = %s WHERE entity = %s''', (json.dumps(data), entity), connect)
            cursor.close()
            connection.close()
    

connect =  get_config()
if __name__ == "__main__":
    parallel = True
    sub_parallel = False

    EN = EntityNarratives(connect)
    narrative_record = EN.get_records()

    if not parallel:
        pbar = tqdm(narrative_record, total=len(narrative_record), desc="Narratives")
        for record in pbar:
            EN.process_narratives(record)
            pbar.update()
            f = open('last_sql.txt', 'w')
            f.write(str(pbar.last_print_n))
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
        
        with Pool(processes=16) as process_pool:
            with tqdm(total=len(narrative_record)) as pbar:
                for i, _ in enumerate(process_pool.imap_unordered(EN.process_narratives, narrative_record)):
                    pbar.update()
                    status = round((pbar.last_print_n/len(narrative_record)) * 100)
        

        print("Finished processing!")

        print("\nClosing pool")
        process_pool.close()
        print("Joining pool")
        process_pool.join()
        print("Clearing pool")
        process_pool.clear()
        print("Finished!")
  

