from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from datetime import datetime
import json
import os

# es = Elasticsearch()

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
folder = './data/'

def read_post(posts, user_id):
    for post in posts:
        post_dic = {
            'user_id': user_id,
            'id': post['id'],
            'story': post['story'],
            'message': post['message'],
            'created_time': post['created_time'],
            'reactions': post['reactions']['data']
            #'comments': post['comments']['data'] if 'comments' in post else []
        }
        print(post_dic)
        es.index(index="facebook", doc_type="post", id=post['id'], body=post_dic)


gender_dict = {"男性":"male", "女性":"female", "male":"male", "female":"female"}

#Retrieve data from file
for file_name in os.listdir(folder):
    # print(file_name)
    with open(folder + file_name) as file:
        # print(file_name)
        for i, line in enumerate(file.readlines()):
            # print(i)
            try:
                data = json.loads(line)
                # print(data['id'])
                try:
                    document = {
                        'id': data['id'],
                        'name': data['name'],
                        'gender': gender_dict[data['gender']]
                    }
                    read_post(data['posts']['data'], data['id'])
                    es.index(index="facebook", doc_type="user", id=data['id'], body=document)
                except:
                    read_post(data['posts']['data'], data['id'])
                    continue
            except:
                # print(line)
                continue
           