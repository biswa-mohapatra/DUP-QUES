import pandas as pd
import numpy as np
import time
import pandas_gbq
from google.cloud import bigquery
import os,os.path
from xmlrpc import client
import json
import db_dtypes
import re
import ast

class duplicate_v1(object):
    def __init__(self):
        self.j_file_path = "examly-events-26d60ca289a0.json"
        self.cleaner = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    
    def get_query(self):
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
                LIMIT 1000
                """
    
    def connect_bigquerry(self):
        start_time = time.time()
        print(f"Connecting the bigquerry...")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']=os.path.expanduser(self.j_file_path)
        print(f"Connected to the bigquerry...\nTime taken :: {time.time() - start_time}")
        
        
    def fetch_data(self):
        start_time = time.time()
        self.connect_bigquerry()
        print(f"Fetching data from the bigquerry...")
        # Fetching the whole data
        client = bigquery.Client()
        query = self.get_query()
        whole_data = pandas_gbq.read_gbq(query, use_bqstorage_api=True,dialect="standard")
        print(f"Data from bigquery downloaded...\nTime taken :: {time.time() - start_time}")
        #whole_data.head(10000).to_excel("Whole_data.xlsx")
        return whole_data
    
    
    def col_to_drop(self,columns:list):
        return self.data.drop(columns,axis=1)
    
    def select_data(self,whole_data,q_type:str):
        return whole_data.iloc[np.where(whole_data["question_type"] == q_type)]
    
    def insert_col(self,data,pos,col_name,value):
        return data.insert(pos,col_name,value)
    
    
    def clean_data(self,data):
        start_time = time.time()
        data["question_data"] = data["question_data"].replace(r"<p></p>",np.NaN,regex=True)
        if not "clean_question_data" in data.columns:
            data.insert(loc=3,column="clean_question_data",value=data["question_data"].str.replace(r'<[^<>]*>','',regex=True).replace("&nbsp","").replace("&amp","").replace('}',''))
            #data["clean_question_data"] = re.sub("&nbsp","",data["clean_question_data"])
            print("Doing regex")
            data["clean_question_data"] = data["clean_question_data"].map(lambda x: re.sub("&nbsp","",str(x)))
            print("Done regex")
        #data["clean_question_data"] = data["question_data"].str.replace(r'<[^<>]*>','',regex=True)
        print(f"Data cleaned...\nTime taken :: {time.time() - start_time}")
        return data
    
     

    def cleanhtml(self,raw_html):
        cleantext = re.sub(self.cleaner, '', raw_html)
        return cleantext
    
    def clean_answer_data(self,data):
        start_time = time.time()
        if "cleaned_mcq_questions_options" in list(data.columns):
            none_idx = []
            print(f"Column present....")
            for i in range(len(data["mcq_questions_options"])):
                sample = data["mcq_questions_options"].iloc[i]
                if not sample == None:
                    size = len(eval(list([sample])[0]))
                    data_ans = []
                    for n in range(size):
                        data_ans.append(self.cleanhtml(list(eval(list([sample])[0])[n].values())[0]))
                    data["cleaned_mcq_questions_options"].iloc[i] = data_ans
                else:
                    none_idx.append(i)
            print(len(none_idx))
            print(f"Answer Data cleaned...\nTime taken :: {time.time() - start_time}")
            return data
        else:
            print("Insert cleaned_mcq_questions_options into your data...")
    
    
    def clean_nan(self,data):
        data = data.drop(index=list(np.where(data["clean_question_data"] == "nan")[0]))
        return data
    
    
    def transform_data(self,cleaned_data):
        start_time = time.time()
        data_new = cleaned_data
        data_new["clean_question_data"] = [str(data_new["clean_question_data"].iloc[i]).strip().lower() for i in range(len(data_new["clean_question_data"]))]
        return data_new
    
    
    def filter_duplicate(self,transformed_data):
        start_time = time.time()
        data = transformed_data
        data["duplicate"] = data["clean_question_data"].duplicated()
        duplicated = data.iloc[np.where(data["duplicate"] == True)]
        return duplicated.reset_index()
    
    def find_uniques(self,transformed_data):
        data = transformed_data
        data["duplicate"] = data["clean_question_data"].duplicated()
        unique = data.iloc[np.where(data["duplicate"] == False)]
        return unique.reset_index()
    
    def prog_question_sol_cleaned(self,data):
        """
        This program will clean the programming question solution data
        and will return a json/dict output.
        """
        try:
            if "cleaned_programing_question_solution" in data.columns:
                print("Column present...")
                for idx in list(data.index):
                #print(data["programing_question_solution"].iloc[idx])
                    if data["question_type"].iloc[idx] == "programming":
                        if not data["programing_question_solution"].iloc[idx] == None:
                            if not data["programing_question_solution"].iloc[idx] == float:
                                #print(row[1]["programing_question_solution"])
                                #count+=1
                                #print(count)
                                data["programing_question_solution"].iloc[idx] = str(data["programing_question_solution"].iloc[idx])
                                scent = data["programing_question_solution"].iloc[idx].replace("null","None").replace("true","True").replace("false","False")
                                scent = re.sub(r"\\r","",scent)
                                scent = re.sub(r"\\n","",scent)
                                scent = re.sub(r"\\\\","",scent)
                                scent = re.sub(r"\\","",scent)
                                scent = re.sub(r"%","",scent)
                                scent = re.sub(r"#","",scent)
                                scent = scent.strip(" ")
                                scent = scent.strip('][').split(", ")
                                final = []
                                for i in range(len(scent)):
                                    scent[i] = scent[i].replace('}',"").replace("{","").replace('"',"").split(":")
                                    final.append({scent[i][0] : scent[i][1:]})
                                data["cleaned_programing_question_solution"].iloc[idx] = final
                print("Cleaned prog quetions.")                
                return data
            else:
                print("Insert cleaned_programing_question_solution column.")
        except Exception as e:
            raise(e)

            
    def prepare_prog_meta_data(self,data):
        """To prepare all the programming meta data"""
        try:
            # Prepare metadata:
            print("Started Preparing meta data")
            for idx in list(data.index):
                # code language
                if data["question_type"].iloc[idx] == "programming":
                    if "language" in data.columns:
                        final = data["cleaned_programing_question_solution"].iloc[idx]
                        if len(final)>1:
                            lang = []
                            for i in range(len(final[0:])):
                                if list(final[i].items())[0][0] == "language":
                                    lang.append(final[i]["language"][0].strip())
                            #print(lang)
                            data["language"].iloc[idx] = lang
                        else:
                            data["language"].iloc[idx] = pd.NA
                    else:
                        print("Add language column to the data..")
                    # solution tags
                    if "solutionbest" in data.columns:
                        sol_scent = data["cleaned_programing_question_solution"].iloc[idx]
                        if len(sol_scent) > 1:
                            sol = []
                            for i in range(len(sol_scent[0:])):
                                if list(sol_scent[i].items())[0][0] == "solutionbest":
                                    sol.append(final[i]["solutionbest"][0])
                            data["solutionbest"].iloc[idx] = sol
                        else:
                            data["solutionbest"].iloc[idx] = pd.NA
                    else:
                        print("Add solutionbest column..")
                    
                    # solution code
                    if "codesnipet" in data.columns:
                        code_scent = data["cleaned_programing_question_solution"].iloc[idx]
                        if len(code_scent)>1:
                            code = []
                            for i in range(len(final[0:])):
                                if list(final[i].items())[0][0] == "solutiondata":
                                    code.append(str(final[i]["solutiondata"][0:][1:]).replace("[]",""))
                            data["codesnipet"].iloc[idx] = code
                        else:
                            data["codesnipet"].iloc[idx] = pd.NA
                    else:
                        print("Add the codesnipet column..")
            return data        
        except Exception as e:
            raise e
                    
    
    
    def find_dup_idx(self,data,scentence:str) -> list:
        return list(np.where(scentence == data["clean_question_data"][::1])[0])
    
    
    # to show the variations:
    def variations(self,filtered_data,idx:list):
        start_time = time.time()
        if len(filtered_data.iloc[idx].index)>1:
            print(f"filtered_data :: {len(filtered_data.iloc[idx].index)}")
            q_type = int(len(filtered_data["question_type"].iloc[idx]))
            #q_type = filtered_data["question_type"].nunique()
            if q_type > 2:
                print(f"q_type :: {q_type}")
                q_sub = int(len(filtered_data["subject"].iloc[idx]))
                #q_sub = filtered_data["subject"].nunique()
                if q_sub > 2:
                    print(f"q_sub :: {q_sub}")
                    q_topic = int(len(filtered_data["topic"].iloc[idx]))
                    #q_topic = filtered_data["topic"].nunique()
                    if q_topic > 2:
                        print(f"q_topic :: {q_topic}")
                        q_sub_topic = int(len(filtered_data["sub_topic"].iloc[idx]))
                        #q_sub_topic = filtered_data["sub_topic"].nunique()
                        if q_sub_topic > 2:
                            print(f"q_sub_topic :: {q_sub_topic}")
                            print(f"Variation found...\nTime taken :: {time.time() - start_time}")
                            return filtered_data.iloc[idx]
        else:
            print(f"No variations found...\nTime taken :: {time.time() - start_time}")
            
    def find_ques_dups(self,data1,data2):
        """Match for question data except programing."""
        try:
            #print("Started finding duplicates")
            if data1["clean_question_data"] == data2["clean_question_data"]:
                if data1["question_type"] != "programming":
                    if data2["question_type"] != "programming":
                        if data1['subject'] == data2['subject']:
                            if data1['topic'] == data2['topic']:
                                if data1['subtopic'] == data2['subtopic']:
                                    if (data1["question_type"] and data2["question_type"]) == "mcq_single_correct" or (data1["question_type"] and data2["question_type"]) == "mcq_multiple_correct":
                                        if data1["cleaned_mcq_questions_options"] == data2["cleaned_mcq_questions_options"]:
                                            return data2["q_id"]
                                    else:
                                        return data2["q_id"]
                else:
                    if data1['subject'] == data2['subject']:
                        if data1['topic'] == data2['topic']:
                            if data1['subtopic'] == data2['subtopic']:
                                if data1["language"] == data2["language"]:
                                    if data1["codesnipet"] == data2["codesnipet"]:
                                        if data1["solutionbest"] == True:
                                            return data1["q_id"]
                                        elif data2["solutionbest"] == True:
                                            return data2["q_id"]
                                        else:
                                            return data2["q_id"]
        except Exception as e:
            raise(e)
    
    def get_dup_unique_ques(self,data,file_name,path):
        """To find the duplicate and unique questions."""
        try:
            start_time = time.time()
            print("Starting getting duplicates and uniques") 
            dup_index = {}
            count = 0
            num = np.random.randint(0,200)
            idxs = list(data.index)
            for idx_1 in idxs:
                #print(data[["clean_question_data","question_type","subject","topic","subtopic"]].iloc[idx_1])
                dup = []
                for idx_2 in idxs:
                    if idx_1 != idx_2:
                        data_1 = data.iloc[idx_1]
                        data_2 = data.iloc[idx_2]
                        result = self.find_ques_dups(data_1,data_2)
                        if result != None:
                            dup.append(result)
                dup_index[data["q_id"].iloc[idx_1]] = dup
            with open(f'{path}data-{file_name}.json', 'w', encoding='utf-8') as f:
                json.dump(dup_index, f, ensure_ascii=False, indent=4)
            #return dup_index
        except Exception as e:
            raise e

            
def pipeline():
    duplicate = duplicate_v1()
    whole_data = pd.read_excel("Files//A.xlsx")
    ## empty answer rows whose question deletion or qb deletion data is absent.
    idx = [] 
    for i in range(len(whole_data["question_data"])):
        if whole_data["q_deletedAt"].iloc[i] is pd.NaT:
            if whole_data["qb_deletedAt"].iloc[i] is pd.NaT:
                if whole_data["mcq_questions_options"].iloc[i] == '[]':
                    idx.append(i)
    
    ## droping if present any empty data:
    if len(idx) > 0:
        whole_data.drop(index=idx)
    
    ## filtering the deleted data from the whole data:
    idx_create = set()
    for i in range(len(whole_data["question_data"])):
        if whole_data["q_deletedAt"].iloc[i] is not pd.NaT:
            idx_create.add(i)
        elif whole_data["qb_deletedAt"].iloc[i] is not pd.NaT:
            idx_create.add(i)
    
    ## Droping the deleted data:
    if len(idx_create) > 0:
        desc = "Y"
        if desc == "Y":
            whole_data.drop(index=list(idx_create))
    #return whole_data
    ## cleaning the data
    print("Data clean step-1")
    cleaned_data = duplicate.clean_data(whole_data)
    
    ## Cleaning the null values:
    print("Data clean step-2")
    cleaned_data_2 = duplicate.clean_nan(cleaned_data)
    
    ## Inserting the required columns:
    print("Adding columns...")
    duplicate.insert_col(cleaned_data_2,18,"cleaned_mcq_questions_options",value="")
    duplicate.insert_col(cleaned_data_2,25,"cleaned_programing_question_solution",value="")
    duplicate.insert_col(cleaned_data_2,26,"language",value="")
    duplicate.insert_col(cleaned_data_2,27,"solutionbest",value="")
    duplicate.insert_col(cleaned_data_2,27,"codesnipet",value="")
    
    ## # transforming the cleaned data
    print("Transforming the data")
    transformed_data = duplicate.transform_data(cleaned_data_2)
    
    ## programming question solution cleaned
    print("Cleaning programming questions.")
    prog_sol_cleaned_data = duplicate.prog_question_sol_cleaned(transformed_data)
    
    ## Prepare the programming meta data
    print("Preparing meta data")
    final_data = duplicate.prepare_prog_meta_data(prog_sol_cleaned_data)
    final_data.fillna("Null")
    #print(final_data.isnull().sum())
    #return final_data
    
    ## Filtering the duplicates:
    print("Finding duplicates.")
    # detect dict or list columns
    #return final_data
    #res = final_data.applymap(lambda x: isinstance(x, dict) or isinstance(x, list)).all()
    #index = list(res.iloc[np.where(res == True)].index)
    #print(index)
    final_data_col = final_data.drop(["q_createdAt","q_updatedAt","q_deletedAt","qb_createdAt","qb_updatedAt","qb_deletedAt","programing_question_solution"],axis=1,inplace=False)
    #return final_data_col
    #res = final_data_col.applymap(lambda x: isinstance(x, dict) or isinstance(x, list)).all()
    #index = list(res.iloc[np.where(res == True)].index)
    final_data_col["is_verified_question"] = final_data_col["is_verified_question"].astype(str)
    for col in list(final_data_col.columns):
        final_data_col[col].replace(np.nan,"None",inplace=True)
        final_data_col[col] = final_data_col[col].apply(str)
        
    #final_data_col["programing_question_solution"] = final_data_col["programing_question_solution"].apply(tuple)
    #return final_data_col
    #final_data_col = final_data_col.head(100)
    file_name = "H"
    path = "Output/"
    duplicate_list = duplicate.get_dup_unique_ques(final_data_col,file_name,path)

if __name__ == "__main__":
    pipeline()