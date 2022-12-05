import pandas as pd
import numpy as np
import time
import pandas_gbq
from google.cloud import bigquery
import os,os.path
from xmlrpc import client
import db_dtypes
import re
import ast


def get_query():
     return f"""
            WITH question_bank_schools as (
            select qbbd.qb_id, qbbd.school_id, s.school_code from `examly_warehouse_mysql_replica.question_bank_branch_department` qbbd
            left join `examly_warehouse_mysql_replica.schools` s
            on s.school_id = qbbd.school_id
            where qbbd.share = 0 and qbbd.deletedAt is null
            group by qbbd.qb_id, qbbd.school_id, s.school_code
            )
            select
            q.qb_id,
            q.q_id,
            q.question_data,
            q.manual_difficulty,
            q.question_type,
            q.imported,
            q.subject_id,
            q.topic_id,
            q.sub_topic_id,
            (select name from `examly_warehouse_mysql_replica.subject` s where subject_id = q.subject_id group by name) as subject,
            (select name from `examly_warehouse_mysql_replica.topic` where topic_id = q.topic_id group by name) as topic,
            (select name from `examly_warehouse_mysql_replica.sub_topic` where sub_topic_id = q.sub_topic_id group by name) as subtopic,
            qbs.school_id,
            qbs.school_code,
            q.createdAt as q_createdAt,
            q.updatedAt as q_updatedAt,
            q.deletedAt as q_deletedAt,
            qb.createdAt as qb_createdAt,
            qb.updatedAt as qb_updatedAt,
            qb.deletedAt as qb_deletedAt,
            mcq.options as mcq_questions_options,
            mcq.answer as mcq_questions_answer,
            IF((select count(qt.id) from `examly_warehouse_mysql_replica.questions_has_tags` qt where qt.q_id = q.q_id and qt.deletedAt is null and qt.tag_id in (1005174,1008357,996154,937692,1007932))>0,True,False) as is_verified_question,
            q.answer_explanation,
            prog.solution as programing_question_solution,
            qb.qb_name,
            from `examly_warehouse_mysql_replica.questions` q
            inner join `examly_warehouse_mysql_replica.question_banks` qb
            on qb.qb_id = q.qb_id
            inner join question_bank_schools qbs
            on qbs.qb_id = qb.qb_id
            and qbs.school_code IN ("neowise")
            left join `examly_warehouse_mysql_replica.mcq_questions` mcq
            on mcq.q_id = q.q_id
            left join `examly_warehouse_mysql_replica.programming_question` prog
            on prog.q_id = q.q_id
            """
    
def connect_bigquerry(j_file_path):
    start_time = time.time()
    print(f"Connecting the bigquerry...")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=os.path.expanduser(j_file_path)
    print(f"Connected to the bigquerry...\nTime taken :: {time.time() - start_time}")
        
        
def fetch_data():
    start_time = time.time()
    connect_bigquerry(j_file_path="examly-events-26d60ca289a0.json")
    print(f"Fetching data from the bigquerry...")
    # Fetching the whole data
    client = bigquery.Client()
    query = get_query()
    whole_data = pandas_gbq.read_gbq(query, use_bqstorage_api=True,dialect="standard")
    print(f"Data from bigquery downloaded...\nTime taken :: {time.time() - start_time}")
    return whole_data





def sampling():
    whole_data = fetch_data(sampling_size)
    n = whole_data.shape[0]//4  #chunk row size
    print(n)
    list_df = [whole_data[i:i+n] for i in range(0,whole_data.shape[0],n)]
    return list_df

def prepare_data(file_path:str,sampling_size:int)->None:
    list_df = sampling(sampling_size)
    file_name_list = ["A","B","C","D","E","F","G","H"]
    if len(file_path) > 0:
        for df in range(0,len(list_df)-1):
            print(f"Saving {file_name_list[df]}.xlsx")
            list_df[df].to_excel(f"{file_path}//{file_name_list[df]}.xlsx")
            print(f"Saved {file_name_list[df]}.xlsx")
