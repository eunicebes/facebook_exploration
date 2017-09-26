from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from datetime import datetime
import json
import os

# es = Elasticsearch()

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
folder = './data/'

def read_post(posts):
    post_arr = []
    for post in posts:
        if 'comments' in post:
            post_dic = {
                'id': post['id'],
                'story': post['story'],
                'message': post['message'],
                'created_time': post['created_time'],
                'reactions': post['reactions']['data'],
                'comments': post['comments']['data']
            }
            post_arr.append(post_dic)
            break
    print(post_arr)

#Retrieve data from file
for file_name in os.listdir(folder):
    # print(file_name)
    with open(folder + file_name) as file:
        for i, line in enumerate(file.readlines()):         
            try:
                data = json.loads(line)
                #print(data['id'])
                post_dic =  read_post(data['posts']['data'])
                break
                # try:
                #     document = {
                #         'id': data['id'],
                #         'name': data['name'],
                #         'gender': data['gender']
                #     }
                #     print(document)
                #     break
                # except:
                #     document = {
                #         'id': data['id'],
                #         'post':{

                #         }
                #     }
                #     print(document)
                #     break
                # To remove the html tags
                # data['content'] = ''.join(BeautifulSoup(data['content']).findAll(text=True))
            except:
                # print(line)
                continue
            # es.index(index="facebook", doc_type=file_name.split('.')[0], id=i+1, body=data)
    break