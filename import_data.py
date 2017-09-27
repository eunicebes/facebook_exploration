from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
from datetime import datetime
import json
import os

# es = Elasticsearch()

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
folder = './data/'
gender_dict = {"男性":"male", "女性":"female", "male":"male", "female":"female"}

def read_post(posts, user_id):
    for post in posts:
        post_dict = {
            'user_id': user_id,
            'id': post['id'],
            'story': post['story'],
            'message': post['message'] if 'message' in post else '',
            'created_time': post['created_time'],
            'reactions': post['reactions']['data']
            #'comments': post['comments']['data'] if 'comments' in post else []
        }
        # print(post_dict)
        es.index(index="facebook", doc_type="post", id=post['id'], body=post_dict)

        if 'comments' in post:
            read_comment(post['comments']['data'], post['id'])

def read_comment(comments, post_id):
    for comment in comments:
        comment_dict = {
            'post_id': post_id,
            'id': comment['id'],
            'message': comment['message'],
            'created_time': comment['created_time'],
            'from': comment['from']
        }
        print(comment_dict)
        es.index(index="facebook", doc_type="comments", id=comment['id'], body=comment_dict)


def import_data():
    # Get all file names
    for file_name in os.listdir(folder):
        # print(file_name)
        with open(folder + file_name) as file:
            for i, line in enumerate(file.readlines()):
                try:
                    data = json.loads(line)
                    # print(data['id'])
                    if 'name' in data and 'gender' in data:
                        document = {
                            'id': data['id'],
                            'name': data['name'],
                            'gender': gender_dict[data['gender']]
                        }
                        # Insert or update user profile in elasticsearch
                        es.index(index="facebook", doc_type="user", id=data['id'], body=document)

                    read_post(data['posts']['data'], data['id'])
                        
                except:
                    # print(line)
                    continue

if __name__ == "__main__":
    
    #Retrieve data from file
    import_data()           