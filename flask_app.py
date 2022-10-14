# The source code of the web application dashboard receives data from spark and saves the data into a redis database

from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

app = Flask(__name__)

# saving the data received from Spark into a Redis database.
# @app.route('/updateData', methods=['POST'])
# def updateData():
#     data = request.get_json()
#     r = Redis(host='redis', port=6379)
#     r.set('data', json.dumps(data))
#     return jsonify({'msg': 'success'})

@app.route('/updateDataUnique', methods=['POST'])
def updateUniqueRepo():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('uniqueRepos', json.dumps(data))
    # r.set('starCount', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDataStarCount', methods=['POST'])
def updateAvgStarCount():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('starCount', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDataPythonWord', methods=['POST'])
def updatePythonWord():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('pythonCount', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDataJavaWord', methods=['POST'])
def updateJavaWord():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('javaCount', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDataRubyWord', methods=['POST'])
def updateRubyWord():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('rubyCount', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDataPyrecent', methods=['POST'])
def updatePyPush():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('pythonPushes', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDataJavrecent', methods=['POST'])
def updateJavPush():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('javaPushes', json.dumps(data))
    return jsonify({'msg': 'success'})

@app.route('/updateDataRubrecent', methods=['POST'])
def updateRubPush():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('rubyPushes', json.dumps(data))
    return jsonify({'msg': 'success'})

# when a user accesses the dashboard it reads data from redis and uses MatPlotlib to create a bar  chart and display the results in real time.
@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data = r.get('uniqueRepos') # reading data from redis
    starCountData = r.get('starCount')
    pythonWordCount = r.get('pythonCount')
    javaWordCount = r.get('javaCount')
    rubyWordCount = r.get('rubyCount')
    
    pythonPushes = r.get('pythonPushes')
    javaPushes = r.get('javaPushes')
    rubyPushes = r.get('rubyPushes')
    
    
    # Storing read data in json format
    try:
        data = json.loads(data)
    except TypeError:
        return "waiting for unique data..."
    
    try:
        starCountData = json.loads(starCountData)
    except TypeError:
        return "waiting for star count data..."
    
    try:
        pythonWordCount = json.loads(pythonWordCount)
    except TypeError:
        return "waiting for Python word count data..."
    
    try:
        javaWordCount = json.loads(javaWordCount)
    except TypeError:
        return "waiting for Java word count data..."
    
    try:
        rubyWordCount = json.loads(rubyWordCount)
    except TypeError:
        return "waiting for Ruby word count data..."
    
    try:
        pythonPushes = json.loads(pythonPushes)
    except TypeError:
        return "waiting for data..."
    
    try:
        javaPushes = json.loads(javaPushes)
    except TypeError:
        return "waiting for data..."
    
    try:
        rubyPushes = json.loads(rubyPushes)
    except TypeError:
        return "waiting for data..."
    
   
   
    # Saving data variables to be displayed in webapp dashboard
    
    try:
        py_stamps = pythonPushes['pushed_time']
        pypush_count = pythonPushes['PYTHON_RECENT_PUSHES']
    except (ValueError, IndexError):
        py_stamps = ["Waiting for Data"]
    
    try:
        jav_stamps = javaPushes['pushed_time']
        javpush_count = javaPushes['JAVA_RECENT_PUSHES']
    except (ValueError, IndexError):
        jav_stamps = ["Waiting for Data"]
    
    try:
        rub_stamps = rubyPushes['pushed_time']
        rubpush_count = rubyPushes['RUBY_RECENT_PUSHES']
    except (ValueError, IndexError):
        rub_stamps = ["Waiting for Data"]
      
      
      
        
    try:
        python_index = data['Language'].index('Python')
        pythonCount = data['Overall_Count'][python_index]
    except ValueError:
        pythonCount = 0
    
    try:
        java_index = data['Language'].index('Java')
        javaCount = data['Overall_Count'][java_index]
    except ValueError:
        javaCount = 0
    
    try:
        ruby_index = data['Language'].index('Ruby')
        rubyCount = data['Overall_Count'][ruby_index]
    except ValueError:
        rubyCount = 0
    
    
    # getting average star count values
    try:
        python_sindex = starCountData['Language'].index('Python')
        pythonStarCount = starCountData['Average_Star_Count'][python_sindex]
    except ValueError:
        pythonStarCount = 0
    
    try:
        java_sindex = starCountData['Language'].index('Java')
        javaStarCount = starCountData['Average_Star_Count'][java_sindex]
    except ValueError:
        javaStarCount = 0
    
    try:
        ruby_sindex = starCountData['Language'].index('Ruby')
        rubyStarCount = starCountData['Average_Star_Count'][ruby_sindex]
    except ValueError:
        rubyStarCount = 0
        
    try:
        pw1 = pythonWordCount['Word'][0] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][0])
    except (ValueError, IndexError):
        pw1 = "Loading word count"
    try:
        pw2 = pythonWordCount['Word'][1] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][1])
    except (ValueError, IndexError):
        pw2 = "Loading word count"
    try:
        pw3 = pythonWordCount['Word'][2] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][2])
    except (ValueError, IndexError):
        pw3 = "Loading word count"
    try:
        pw4 = pythonWordCount['Word'][3] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][3])
    except (ValueError, IndexError):
        pw4 = "Loading word count"
    try:
        pw5 = pythonWordCount['Word'][4] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][4])
    except (ValueError, IndexError):
        pw5 = "Loading word count"
    try:
        pw6 = pythonWordCount['Word'][5] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][5])
    except (ValueError, IndexError):
        pw6 = "Loading word count"
    try:
        pw7 = pythonWordCount['Word'][6] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][6])
    except (ValueError, IndexError):
        pw7 = "Loading word count"  
    try:
        pw8 = pythonWordCount['Word'][7] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][7])
    except (ValueError, IndexError):
        pw8  = "Loading word count"
    try:
        pw9 = pythonWordCount['Word'][8] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][8])
    except (ValueError, IndexError):
        pw9 = "Loading word count"
    try:
        pw10 = pythonWordCount['Word'][9] + ", "+ str(pythonWordCount['Number_of_Occurences_for_Python'][9])
    except (ValueError, IndexError):
        pw10 = "Loading word count"
     
    # Storing Java description words count
        
    try:
        j1 = javaWordCount['Word'][0] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][0])
    except (ValueError, IndexError):
        j1 = "Loading word count"
    try:
        j2 = javaWordCount['Word'][1] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][1])
    except (ValueError, IndexError):
        j2 = "Loading word count"
    try:
        j3 = javaWordCount['Word'][2] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][2])
    except (ValueError, IndexError):
        j3 = "Loading word count"
    try:
        j4 = javaWordCount['Word'][3] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][3])
    except (ValueError, IndexError):
        j4 = "Loading word count"
    try:
        j5 = javaWordCount['Word'][4] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][4])
    except (ValueError, IndexError):
        j5 = "Loading word count"
    try:
        j6 = javaWordCount['Word'][5] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][5])
    except (ValueError, IndexError):
        j6 = "Loading word count"
    try:
        j7 = javaWordCount['Word'][6] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][6])
    except (ValueError, IndexError):
        j7 = "Loading word count"  
    try:
        j8 = javaWordCount['Word'][7] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][7])
    except (ValueError, IndexError):
        j8  = "Loading word count"
    try:
        j9 = javaWordCount['Word'][8] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][8])
    except (ValueError, IndexError):
        j9 = "Loading word count"
    try:
        j10 = javaWordCount['Word'][9] + ", "+ str(javaWordCount['Number_of_Occurences_for_Java'][9])
    except (ValueError, IndexError):
        j10 = "Loading word count"
    

    try:
        r1 = rubyWordCount['Word'][0] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][0])
    except (ValueError, IndexError):
        r1 = "Loading word count"
    try:
        r2 = rubyWordCount['Word'][1] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][1])
    except (ValueError, IndexError):
        r2 = "Loading word count"
    try:
        r3 = rubyWordCount['Word'][2] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][2])
    except (ValueError, IndexError):
        r3 = "Loading word count"
    try:
        r4 = rubyWordCount['Word'][3] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][3])
    except (ValueError, IndexError):
        r4 = "Loading word count"
    try:
        r5 = rubyWordCount['Word'][4] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][4])
    except (ValueError, IndexError):
        r5 = "Loading word count"
    try:
        r6 = rubyWordCount['Word'][5] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][5])
    except (ValueError, IndexError):
        r6 = "Loading word count"
    try:
        r7 = rubyWordCount['Word'][6] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][6])
    except (ValueError, IndexError):
        r7 = "Loading word count"  
    try:
        r8 = rubyWordCount['Word'][7] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][7])
    except (ValueError, IndexError):
        r8  = "Loading word count"
    try:
        r9 = rubyWordCount['Word'][8] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][8])
    except (ValueError, IndexError):
        r9 = "Loading word count"
    try:
        r10 = rubyWordCount['Word'][9] + ", "+ str(rubyWordCount['Number_of_Occurences_for_Ruby'][9])
    except (ValueError, IndexError):
        r10 = "Loading word count"
    
    push_times = []
    push_counts = []
    
    # Define data values
    x = [7, 14, 21, 28, 35, 42, 49]
    y = [5, 12, 19, 21, 31, 27, 35]
    z = [3, 5, 11, 20, 15, 29, 31]

    # Plot a simple line chart
    # plt.plot(pypush_count, javpush_count, 'g', label='Line y')

    # # Plot another line on the same chart/graph
    # plt.plot(pypush_count, javpush_count, 'r', label='Line z')
    # plt.legend()
    # plt.savefig('streaming/static/images/linechart.png')
    
    x = [1, 2, 3]
    height = [pythonStarCount, javaStarCount, rubyStarCount]
    tick_label = ['Python', 'Java', 'Ruby']
    
    plt.figure(figsize=(10, 6))
    plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:green', 'tab:blue', 'tab:red'])
    plt.ylabel('Average number of stars')
    plt.xlabel('PL')
    plt.title('Average Number of Stars per Programming Language')
    # plt.savefig('streaming/images/chart.png')
    plt.savefig('streaming/static/images/chart.png')
    return render_template('index.html', url='static/images/chart.png', pythonCount=pythonCount, javaCount=javaCount, rubyCount=rubyCount,\
                           pw1=pw1, pw2=pw2, pw3=pw3, pw4=pw4, pw5=pw5, pw6=pw6, pw7=pw7, pw8=pw8, pw9=pw9, pw10=pw10,
                           j1=j1, j2=j2, j3=j3, j4=j4, j5=j5, j6=j6, j7=j7, j8=j8, j9=j9, j10=j10, 
                           r1=r1, r2=r2, r3=r3, r4=r4, r5=r5, r6=r6, r7=r7, r8=r8, r9=r9, r10=r10)
    

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')  
