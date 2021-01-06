from elasticsearch import Elasticsearch
import json
import time
from elasticsearch import helpers


class Es():
    def get_client(self, ip):
        count = 0
        while True:
            count += 1
            try:
                client = Elasticsearch([
                    {'host': ip},
                ])
                return client
            #Error handeling
            except Exception as e:
                time.sleep(3)
                count += 1
                if count > 10: 
                    print("Failed to connect to Elasticsearch {} times in a row".format(count))
                else: 
                    # Uncaught errors
                    raise Exception("We aren't catching this Elasticsearch get_client Error: {}".format(e))

    def bulk_request(self, client, actions):
        count = 0
        while True:
            count += 1
            try:
                bulk_action = helpers.bulk(client, actions, request_timeout=500)
                # bulk_action = helpers.parallel_bulk(client, actions, max_chunk_bytes=10485760000, chunk_size=10000, thread_count=10)
                return bulk_action
            #Error handeling
            except Exception as e:
                time.sleep(1)
                count += 1
                if count > 100: 
                    print("Failed to work on Bulk {} times in a row".format(count))
                # else: 
                #     # Uncaught errors
                #     raise Exception("We aren't catching this Elasticsearch bulk_request Error: {}".format(e))

    # Delete
    def delete_record(self, client, index, record_id, doc_type):
        try:
            client.delete(
                index=index,
                # doc_type=doc_type,
                id=record_id
            )
            return True
        except Exception as e:
            # Catching error
            # err = json.loads(e.error)
            # error_message = err['result'] if 'result' in err else e.error
            # print(error_message)
            return False

    # Update
    def update_record(self, client, index, record_id, doc_type, json_body):
        try:
            client.update(
                index=index,
                # doc_type=doc_type,
                id=record_id,
                body={
                    "doc": json_body
                },
                request_timeout=30
            )
            return True
        except Exception as e:
            # Catching error
            if 'document_missing_exception' in str(e.error):
                return False

    # Insert
    def insert_record(self, client, index, record_id, doc_type, json_body):
        response = client.index(
            index=index,
            # doc_type=doc_type,
            id=record_id,
            body=json_body,
            request_timeout=30
        )
        result = response['result'] if 'result' in response else None
        if result == 'created':
            return True
        else:
            return False

    def index_record(self, client, index, record_id, doc_type, json_body):
        response = client.index(
            index=index,
            doc_type=doc_type,
            id=record_id,
            body=json_body,
            request_timeout=30
        )
        result = response['result'] if 'result' in response else None
        if result == 'created':
            return True
        else:
            return False

    # Search
    def search_record(self, client, index, json_request):
        count = 0
        while True:
            count += 1
            try:
                response = client.search(
                    index=index,
                    body=json_request,
                    request_timeout=30
                )
                return response
            except Exception as e:
                time.sleep(1)
                count += 1
                if count > 100:
                    print(
                        "Failed to get search result {} times in a row and error is --- {}".format(count, str(e)))

    # Scroll

    def scroll(self, client, index, json_request):
        result = []
        errors = []

        response = client.search(
            index=index,
            body=json_request,
            scroll='2s',  # length of time to keep search context,
            request_timeout=30
        )

        while True:
            result_hits = response['hits']['hits']
            if result_hits:
                for x in result_hits:
                    source = x['_source']
                    if 'blogpost_id' in source:
                        if source['blogpost_id'] not in result:
                            result.append(source)

                scroll_id = response['_scroll_id']

                try:
                    response = client.scroll(
                        scroll_id=scroll_id,
                        scroll='10s'
                    )
                except Exception as e:
                    errors.append(e)

            else:
                break

        return result


# json_body = {
#     "entity": "Kravchenko",
#     "data": [
#         {
#             "narrative": "Naval Staff Admiral Viktor Kravchenko told Russia Today.",
#             "blogpost_ids": [
#                 11196
#             ]
#         },
#         {
#             "narrative": "favor were needed for Kravchenko to speak.Before the meeting.",
#             "blogpost_ids": [
#                 502093
#             ]
#         },
#         {
#             "narrative": "Russian Navy Admiral Viktor Kravchenko dismissed the U.S. announcement as a. the Russian news agency Interfax.They didnt build new ships.",
#             "blogpost_ids": [
#                 502560
#             ]
#         },
#         {
#             "narrative": "Igor Kravchenko was not the right hire.",
#             "blogpost_ids": [
#                 569962
#             ]
#         },
#         {
#             "narrative": "1966 Victor Kravchenko foi encontrado morto com um tiro na cabeça no seu apartamento em Nova Iorque.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "autor do livro The Kravchenko Case One Mans War on Stalin levanta a dúvida sobre a possibilidade.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "em termos políticos Victor Kravchenko revelou se adepto do império russo.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "a extradição do Victor Kravchenko diretamente ao presidente Roosevelt.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "Victor Kravchenko processou o jornal por difamação.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "Varlam Shalamov.Victor Kravchenko se formou em engenharia na Universidade.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "Andrew Kravchenko produziu um documentário chamado The Defector.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "por agentes soviéticos.Victor Kravchenko ficou conhecido em todo.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "Victor Kravchenko apareceu em.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "Uma das testemunhas do Kravchenko foi a sobrevivente dos campos.",
#             "blogpost_ids": [
#                 666536
#             ]
#         },
#         {
#             "narrative": "No entanto o autor garante que o livro é o fruto das suas vivências reais na Ucrânia contemporânea e. UkrainianTrilogia sobre Ucrania do Roman RijkaVictor Kravchenko Escolhi a LiberdadeHolodomor desconhecida tragédia documental.",
#             "blogpost_ids": [
#                 666758
#             ]
#         },
#         {
#             "narrative": "organizador da ação Leonid Kuzmin na foto em cima e dois participantes Oleksandr Kravchenko jovem levado pela polícia nas fotos em baixo e Vilidar Shukurdjaev.",
#             "blogpost_ids": [
#                 667137
#             ]
#         },
#         {
#             "narrative": "Paris no processo Kravchenko em apoio ao não retornado soviético.",
#             "blogpost_ids": [
#                 667765
#             ]
#         },
#         {
#             "narrative": "Naval Staff Admiral Viktor Kravchenko told Russia Today.Pointedly.",
#             "blogpost_ids": [
#                 851620
#             ]
#         },
#         {
#             "narrative": "Internal Affairs Yuriy Kravchenko repeated this information.",
#             "blogpost_ids": [
#                 1108796
#             ]
#         },
#         {
#             "narrative": "This Kravchenko was Pukachs superior.",
#             "blogpost_ids": [
#                 1108796
#             ]
#         },
#         {
#             "narrative": "Mr. Kravchenko has said that state television.",
#             "blogpost_ids": [
#                 1454443
#             ]
#         }
#     ]
# }

# es = Es()
# client = es.get_client('144.167.135.89')
# es.update_record(client, 'entity_narratives_testing', 'Kravchenko', '_doc', json_body)
    # delete_record('blogposts', 1, '_doc')
    # update_record('blogposts', 1, '_doc', json_body)
    # insert_record('blogposts', 1, '_doc', json_body)

    # search_body = {
    #     "size": 1000,
    #     "query": {
    #         "bool": {
    #             "must_not": [
    #                 {
    #                     "terms": {
    #                         "blogpost_id": [
    #                             1
    #                         ],
    #                         "boost": 1.0
    #                     }
    #                 }
    #             ]
    #         }
    #     }
    # }

    # result = search_record(client, search_body)
    # pass
