import sys
from numpy import number
import requests
import json
from pyspark.sql.functions import countDistinct, avg
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from datetime import timezone, datetime



def aggregate_count(new_values, total_sum):
	  return sum(new_values) + (total_sum or 0)
    
    
def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']

# helper function sending data to dashboard
def send_df_to_dashboard(df, version):
    url = 'http://webapp:5000/updateData{}'.format(version)
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)


# processing and transforming resilient distributed dataset to obtain overall unique repository language count
def process_unique_rdd(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- OVERALL UNIQUE REPOSITORY LANGUAGE COUNT ----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Repo_name=w[0][0], Language=w[0][1], Count=w[0][2]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("Results")
        new_results_df = sql_context.sql("""
                                         SELECT Language, COUNT(*) AS Overall_Count
                                         FROM Results
                                         GROUP BY Language
                                         ORDER BY 2
                                         """)
        new_results_df.show()
        send_df_to_dashboard(new_results_df, "Unique")
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
    
# processing and transforming resilient distributed dataset to obtain count of repositories pushed in the last 60s    
def per_batch_count(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- COUNT OF REPOSITORIES PUSHED IN THE LAST 60s----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(pushed_time=w[0], time_lapse=w[1], Language=w[2]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("""
                                         SELECT COUNT(*) AS NUMBER_OF_MOST_RECENT_REPOSITORIES
                                         FROM results
                                         WHERE Language = "Python" OR Language = "Java" OR Language = "Ruby"
                                         """)
        new_results_df.show()
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# processing and transforming resilient distributed dataset to obtain count of repositories pushed in the last 60s per language
def recent_per_language(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- REQUIREMENT 3ii and v: COUNT OF REPOSITORIES PUSHED IN THE LAST 60s PER LANGUAGE----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(pushed_time=w[0], time_lapse=w[1], Language=w[2]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("""
                                         SELECT Language, COUNT(pushed_time) AS NUMBER_OF_MOST_RECENT_REPOSITORIES
                                         FROM results
                                         GROUP BY Language
                                         ORDER BY 2 DESC                                        
                                         """)
        new_results_df.show()
        #  WHERE Language = "Python" OR Language = "Java" OR Language = "Ruby"
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


# processing and transforming resilient distributed dataset to obtain count of python repositories pushed within the last 60s
def recent_python(time, rdd):
    
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- COUNT OF PYTHON REPOSITORIES PUSHED IN THE LAST 60s----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(pushed_time=w[0], Language=[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("""
                                         SELECT pushed_time, COUNT(pushed_time) AS PYTHON_RECENT_PUSHES
                                         FROM results
                                         GROUP BY pushed_time
                                         
                                         """)
        new_results_df.show()
        send_df_to_dashboard(new_results_df, "Pyrecent")
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# processing and transforming resilient distributed dataset to obtain count of Java repositories pushed within the last 60s
def recent_java(time, rdd):
    
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- COUNT OF JAVA REPOSITORIES PUSHED IN THE LAST 60s----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(pushed_time=w[0], Language=[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("""
                                         SELECT pushed_time, COUNT(pushed_time) AS JAVA_RECENT_PUSHES
                                         FROM results
                                         GROUP BY pushed_time
                                         
                                         """)
        new_results_df.show()
        send_df_to_dashboard(new_results_df, "Javrecent")
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# processing and transforming resilient distributed dataset to obtain count of Ruby repositories pushed within the last 60s
def recent_ruby(time, rdd):
    
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- COUNT OF RUBY REPOSITORIES PUSHED IN THE LAST 60s----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(pushed_time=w[0], Language=[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql("""
                                         SELECT pushed_time, COUNT(pushed_time) AS RUBY_RECENT_PUSHES
                                         FROM results
                                         GROUP BY pushed_time
                                         
                                         """)
        new_results_df.show()
        send_df_to_dashboard(new_results_df, "Rubrecent")
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        

# processing and transforming resilient distributed dataset to obtain average stargazers count per language
def star_count(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- OVERALL AVERAGE LANGUAGE STAR COUNT ----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Repo_Name=w[0][0], Language=w[0][1], Star_Count=w[0][2]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("results")
        new_results_df = sql_context.sql(""" 
                                         SELECT Language, ROUND(AVG(Star_Count), 2) AS Average_Star_Count 
                                         FROM results 
                                         GROUP BY Language
                                         ORDER BY 2 
                                        """)
        new_results_df.show()
        send_df_to_dashboard(new_results_df, "StarCount")
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
  

# processing and transforming resilient distributed dataset to obtain top 10 words in repositories using Python as primary language
def processPythonWord(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- TOP 10 WORD COUNT FOR PYTHON LANGUAGE ----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Word = w))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("Results")
        new_results_df = sql_context.sql("""
                                         SELECT Word, COUNT(Word) AS Number_of_Occurences_for_Python
                                         FROM Results
                                         GROUP BY Word
                                         ORDER BY 2 DESC
                                         LIMIT 10
                                         """)
        new_results_df.show()
        send_df_to_dashboard(new_results_df, "PythonWord")
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# processing and transforming resilient distributed dataset to obtain top 10 words in repositories using Java as primary language
def processJavaWord(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- TOP 10 WORD COUNT FOR JAVA LANGUAGE ----------------")
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Word = w))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("Results")
        new_results_df = sql_context.sql("""
                                         SELECT Word, COUNT(Word) AS Number_of_Occurences_for_Java
                                         FROM Results
                                         GROUP BY Word
                                         ORDER BY 2 DESC
                                         LIMIT 10
                                         """)
        new_results_df.show()
        send_df_to_dashboard(new_results_df, "JavaWord")
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
    
# processing and transforming resilient distributed dataset to obtain top 10 words in repositories using Ruby as primary language
def processRubyWord(time, rdd):
    pass
    print("----------- %s -----------" % str(time))
    print("--------------- TOP 10 WORD COUNT FOR RUBY LANGUAGE ----------------")
    
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(Word = w))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("Results")
        new_results_df = sql_context.sql("""
                                         SELECT Word, COUNT(Word) AS Number_of_Occurences_for_Ruby
                                         FROM Results
                                         GROUP BY Word
                                         ORDER BY 2 DESC
                                         LIMIT 10
                                         """)
        new_results_df.show()
        send_df_to_dashboard(new_results_df, "RubyWord")
        
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        
# Helper function to get most recent repositories i.e repository pushed within the last 60s.
def isMostRecent(repo_time):
    my_time = str(datetime.now()).split(".")[0]
    rep_time = int(datetime.strptime(repo_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
    current_time = int(datetime.strptime(my_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
    
    if (current_time - rep_time) >= 0 and (current_time - rep_time) < 60:
        return True
    else:
        return False

def gap(repo_time):
    my_time = str(datetime.now()).split(".")[0]
    rep_time = int(datetime.strptime(repo_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
    current_time = int(datetime.strptime(my_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
    
    gap_value = current_time - rep_time
    if isMostRecent(repo_time) == True:
        return gap_value
        
        
    
if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source" # host name of data source container
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="GitAnalysis") # spark context to connect to spark cluster
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60) # batch interval set to 60 seconds
    ssc.checkpoint("checkpoint_GitAnalysis")
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    lines = data.map(lambda e: e.split(';'))
     
    # Unique repositories are selected by reducing the dstream by key then 
    # updating each key in a tuple of ((repository name, language, int(1)), 1)
    # with each new batch, new keys are added to the tuple in the state using updateStateByKey, and existing ones are unchanged. 
    # the value of the tuple is updated by using the aggregate_count function without modifying the keys and that value is ignored. 
    # The new set of rdd is then sent to be processed and counted in the process_unique_rdd function 
    # 
    repos = lines.map(lambda a: (a[3], a[0])).reduceByKey(lambda a, b: a) # obtaining unique repositories by key in the batch
    repos = repos.map(lambda a: ((a[0], a[1], 1), 1))
    
    updated_repos = repos.updateStateByKey(aggregate_count) # obtaining unique repositories since the start of the streaming
    # duplicate_repos = updated_repos.filter(lambda a: a[1] > 1)
    # duplicate_repos.pprint()
    updated_repos.foreachRDD(process_unique_rdd)
    
    # ------------- UNIQUE REPOSITORY COUNT PER LANGUAGE PER 60s BATCH ---------------
    
    unique_repos = lines.map(lambda a: (a[3], (a[0], int(a[4]), a[5], a[1]))).reduceByKey(lambda a, b: a) # map to (repository name, (language, star count, description)) then reduce by repo name
    # unique_lang_count = unique_repos.map(lambda x: (x[1][0], 1)).reduceByKey(lambda a, b: a + b) # map by language then reduce by adding sum of occurences
    # unique_lang_count.foreachRDD(per_batch_count)
    recent_pushes = unique_repos.filter(lambda t: isMostRecent(t[1][3]) == True)
    recent_pushes = recent_pushes.map(lambda t: (t[1][3], gap(t[1][3]), t[1][0]))
    recent_pushes.foreachRDD(per_batch_count)
    recent_pushes.foreachRDD(recent_per_language)
    # recent_pushes.pprint()
    
    star_repos = unique_repos.map(lambda a: ((a[0], a[1][0], int(a[1][1])), 1)) # map to (repository name, (language, star count, description)) then reduce by repo (repository_name)
    average_star_repos = star_repos.updateStateByKey(aggregate_count)
    average_star_repos.foreachRDD(star_count)
    
    # Obtaining word counts of python descriptions
    python_description = unique_repos.filter(lambda d: d[1][0] == "Python")
    python_description = python_description.map(lambda a: ((a[0], a[1][0], a[1][2]), 1))
    unique_python_description = python_description.updateStateByKey(aggregate_count)
    unique_python_description = unique_python_description.filter(lambda f: f[0][2].strip() != 'None')
    unique_python_description = unique_python_description.filter(lambda f: f[0][2].strip() != None)
    unique_python_description = unique_python_description.filter(lambda f: f[0][2].strip() != "")
    flat_python_description = unique_python_description.flatMap(lambda p: p[0][2].strip().split(" "))
    flat_python_description = flat_python_description.filter(lambda w: len(w.strip()) > 0)
    flat_python_description.foreachRDD(processPythonWord)
    
    # Obtaining word counts of java descriptions
    java_description = unique_repos.filter(lambda d: d[1][0] == "Java")
    java_description = java_description.map(lambda a: ((a[0], a[1][0], a[1][2]), 1))
    unique_java_description = java_description.updateStateByKey(aggregate_count)    
    unique_java_description = unique_java_description.filter(lambda f: f[0][2].strip() != 'None')
    unique_java_description = unique_java_description.filter(lambda f: f[0][2].strip() != None)
    unique_java_description = unique_java_description.filter(lambda f: f[0][2].strip() != "")
    flat_java_description = unique_java_description.flatMap(lambda p: p[0][2].strip().split(" "))
    flat_java_description = flat_java_description.filter(lambda w: len(w.strip()) > 0)
    flat_java_description.foreachRDD(processJavaWord)
    
    # Obtaining word counts of ruby descriptions
    ruby_description = unique_repos.filter(lambda d: d[1][0] == "Ruby")
    ruby_description = ruby_description.map(lambda a: ((a[0], a[1][0], a[1][2]), 1))
    unique_ruby_description = ruby_description.updateStateByKey(aggregate_count)    
    unique_ruby_description = unique_ruby_description.filter(lambda f: f[0][2].strip() != 'None')
    unique_ruby_description = unique_ruby_description.filter(lambda f: f[0][2].strip() != None)
    unique_ruby_description = unique_ruby_description.filter(lambda f: f[0][2].strip() != "")
    flat_ruby_description = unique_ruby_description.flatMap(lambda p: p[0][2].strip().split(" "))
    flat_ruby_description = flat_ruby_description.filter(lambda w: len(w.strip()) > 0)
    flat_ruby_description.foreachRDD(processRubyWord)
    
    
    recent_pushes_lang = recent_pushes.map(lambda t: (t[0], t[2]))
    recent_python_pushes = recent_pushes_lang.filter(lambda l: l[1] == "Python")
    recent_python_pushes.foreachRDD(recent_python)
    
    recent_java_pushes = recent_pushes_lang.filter(lambda l: l[1] == "Java")
    recent_java_pushes.foreachRDD(recent_java)
    
    recent_ruby_pushes = recent_pushes_lang.filter(lambda l: l[1] == "Ruby")
    recent_ruby_pushes.foreachRDD(recent_ruby)
    
    ssc.start()
    ssc.awaitTermination()
    

    
    
