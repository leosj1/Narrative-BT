"""
'''
Modified on Aug 1, 2020
@author: oljohnson

This is to populate narrative for each posts from blogposts table
'''
"""

# Import required libraries
from nltk import tokenize
import nltk
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('stopwords')
import re
import ast
import json
import mysql.connector
from pathos.multiprocessing import freeze_support, ProcessPool
import sys
from tqdm import tqdm
import collections, functools, operator 
from builtins import dict
from sql import SqlFuncs
from functions import pos_tag_narratives, run_comprehensive, entity_narratives, get_config

# connect = 'cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers'
connect =  get_config()
s = SqlFuncs(connect)
connection = s.get_connection(connect)

def process_narratives(connect, parallel, num_processes):
    # Initializing variables
    countPosts = 0

    with connection.cursor() as cursor:
        # Getting blogpost_id and posts from blogposts table
        # query = f"""SELECT blogpost_id, blogsite_id, post FROM blogtrackers.blogposts where blogpost_id not in (select blogpost_id from narratives)"""
        query = f"""SELECT blogpost_id, post, blogsite_id FROM blogtrackers.blogposts limit 40700, 10000000000"""
        # query = f"""SELECT blogpost_id, post, blogsite_id FROM blogtrackers.blogposts limit 16904, 1000"""
        cursor.execute(query)
        records = cursor.fetchall()
        countPosts = countPosts + 1

    connection.close()
    cursor.close() 


    if not parallel:
        for record in tqdm(records, total=len(records), desc="Narratives"):
            process_posts(record)            
    else:
        print("starting multi-process")
        process_pool = ProcessPool(num_processes) 
        pbar = tqdm(process_pool.imap(process_posts, records), desc="Narratives", ascii=True,  file=sys.stdout, total=len(records))
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

    


def process_posts(record):
    from nltk import tokenize
    import re
    import ast
    import json
    import mysql.connector
    from pathos.multiprocessing import freeze_support, ProcessPool
    import sys
    from tqdm import tqdm
    import collections, functools, operator 
    from builtins import dict
    from sql import SqlFuncs
    from functions import pos_tag_narratives, run_comprehensive, entity_narratives, get_config

    # connect = 'cosmos-1.host.ualr.edu', 'ukraine_user', 'summer2014', 'blogtrackers'
    connect = get_config()
    s = SqlFuncs(connect)
    connection = s.get_connection(connect)

    blogpostID = record['blogpost_id']
    post = record['post']
    ListSentences_Unique = []
    entity_narrative_dict_list = []
    countSentTotal = 0
    countSentFiltered = 0
    countSentFilteredTriplet = 0
    textSentString = ''

    stop_words = []
    with open("stopwords.txt", "r", encoding="utf-8") as f:
        for line in f:
            if line != '':
                stop_words.append(str(line.strip()))
            
    new_stp_wrds = []
    
    final_stp_wrds = stop_words + new_stp_wrds
    stopWords = final_stp_wrds

    with connection.cursor() as cursor:
        # Declaring the entities for the blogpost
        cursor.execute(f"SELECT distinct entity from blogpost_entitysentiment where blogpost_id = {blogpostID}")
        records_entity = cursor.fetchall()
        objectEntitiesList = [x['entity'] for x in records_entity if x['entity'].lower() not in stopWords]

    connection.close()
    cursor.close()

    for everyPost in tokenize.sent_tokenize(post):
        countSentTotal = countSentTotal + 1
        everyPost = everyPost.replace("’s", "s")
        """ Clean up activity"""
        everyPost = re.sub(r"[-()\"#/@;:<>{}`'’‘“”+=–—_…~|!?]", " ", everyPost)
        if('on Twitter' not in everyPost and 'or e-mail to:' not in everyPost and 'd.getElementsByTagName' not in everyPost and len(everyPost)>10 and 'g.__ATA.initAd' not in everyPost and 'document.body.clientWidth' not in everyPost):
            countSentFiltered = countSentFiltered +1
            if everyPost not in ListSentences_Unique:
                ListSentences_Unique.append(everyPost)
                textSentString += str(' ') + str(everyPost)
                countSentFilteredTriplet = countSentFilteredTriplet + 1
    
    ListSentences_Unique = []       
    tfidf_string = pos_tag_narratives(textSentString) #takes so much time
    result_scored = run_comprehensive(tfidf_string)
    sentences_scored = tokenize.sent_tokenize(result_scored)
    
    entity_count = []
    data_narratives = entity_narratives(sentences_scored, blogpostID, objectEntitiesList, entity_count)

    if 'Duplicate entry' in s.update_insert('''INSERT INTO narratives (blogpost_id, blogsite_id, narratives, entity_count) values (%s, %s, %s, %s) ''', (blogpostID, record['blogsite_id'], json.dumps(data_narratives), json.dumps(entity_count)), connect):
        s.update_insert('''UPDATE narratives SET narratives=%s, entity_count = %s, blogsite_id = %s WHERE blogpost_id=%s;  ''', (json.dumps(data_narratives), json.dumps(entity_count), record['blogsite_id'], blogpostID), connect)

if __name__ == "__main__":
    parallel = True
    num_processes = 24
    process_narratives(connect, parallel, num_processes)