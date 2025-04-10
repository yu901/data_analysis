from langchain_community.utilities import SQLDatabase
# from langchain_openai import AzureChatOpenAI
# from langchain_openai import 
from langchain.chains.sql_database.query import create_sql_query_chain
from timeit import default_timer as timer
import traceback
import os
import re
from dotenv import load_dotenv
from config import MysqlConfig

mysql_config = MysqlConfig()

import os
api_key = os.environ.get('OPENAI_API_KEY')

def resetenv(key):
    if os.getenv(key):
        del os.environ[key]

def load_environment():
    resetenv("AZURE_OPENAI_ENDPOINT")
    resetenv("AZURE_OPENAI_API_VERSION")
    resetenv("CHAT_COMPLETION_NAME")
    load_dotenv(dotenv_path="./.env")

def get_msdb(table):
    db_uri = mysql_config.get_url() 
    return SQLDatabase.from_uri(db_uri, include_tables=[table])

def extract_sql(generated_sql) :
    pattern = ""
    if "SQLQuery:" in generated_sql:
        pattern = "SQLQuery: \"(.*)\""
    if "```sql" in generated_sql:
        pattern = "```sql\n((?:.|\n)*);?\n```"
    if len(pattern) > 0:
        result = re.findall(pattern, generated_sql)
        if len(result) > 0:
            generated_sql = result[0]
            generated_sql = generated_sql.replace("\"","")
            generated_sql = generated_sql.replace("\\","")
            print("extract generated sql : ", generated_sql)
            return generated_sql
        else:
            return None
        
# @retry(tries=5, delay=5)
def get_sql(table, question):
    start = timer()
    
    try:
        msdb = get_msdb(table)  
       
        if "OPENAI_API_BASE" in os.environ:
            del os.environ["OPENAI_API_BASE"]
        generated_sql, query = __get_sql(question, msdb)
        
        return query 
    except Exception as ex:
        traceback.print_exc()
        print(ex)
    #     return "LLM Quota Exceeded. Please try again"
    finally:
        print('Cypher Generation Time : {}'.format(timer() - start))


def __get_sql(question, msdb):
    llm = AzureChatOpenAI(
            model_name=os.getenv("CHAT_COMPLETION_NAME"),
            openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
            openai_api_key= os.getenv("AZURE_OPENAI_KEY"),  
            azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT"), 
            temperature=0
        )
    print("key value : ", llm.openai_api_key)
    print("model name : ", llm.model_name)
    print("azure endpoint : ", llm.azure_endpoint)
    print("api version : ", llm.openai_api_version)

    chain = create_sql_query_chain(llm, msdb,)

    generated_sql = chain.invoke({"question" : question })

    print("generated sql : ", generated_sql)
    query = extract_sql(generated_sql)
    return generated_sql,query

if __name__ == '__main__':
    import os
    from groq import Groq
    # https://console.groq.com/docs/quickstart

    load_dotenv(dotenv_path="./.env")
    client = Groq()

    # chat_completion = client.chat.completions.create(
    #     messages=[
    #         {
    #             "role": "user",
    #             "content": "Please explain the difference between the audiences and the movie theater regarding seat occupancy in movie theaters.",
    #         }
    #     ],
    #     model="llama3-8b-8192",
    # )

    # print(chat_completion.choices[0].message.content)

    from langchain_core.prompts import ChatPromptTemplate
    from langchain_groq import ChatGroq

    chat = ChatGroq(
        temperature=0,
        model="llama3-70b-8192",
        # api_key="" # Optional if not set as an environment variable
    )

    system = "You are a helpful assistant."
    human = "{text}"
    prompt = ChatPromptTemplate.from_messages([("system", system), ("human", human)])

    chain = prompt | chat
    chain.invoke({"text": "Explain the importance of low latency for LLMs."})

    from typing import Optional

    from langchain_core.tools import tool


    @tool
    def get_current_weather(location: str, unit: Optional[str]):
        """Get the current weather in a given location"""
        return "Cloudy with a chance of rain."


    tool_model = chat.bind_tools([get_current_weather], tool_choice="auto")

    res = tool_model.invoke("What is the weather like in San Francisco and Tokyo?")

    res.tool_calls