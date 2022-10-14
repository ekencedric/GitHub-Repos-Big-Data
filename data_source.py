import requests
import os
import sys
import socket
import random
import time
import re
        
# establishing a TCP socket which is listening to all interfaces and ports 9999

TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")

conn, addr = s.accept()
print("Connected... Starting sending data.")


while True:
    try:
       
        languages = ['Python', 'Java', 'Ruby']
        token = os.getenv('TOKEN')
        
        python_url = 'https://api.github.com/search/repositories?q=+language:Python&sort=updated&order=desc&per_page=50'
        python_repos = requests.get(python_url, headers={"Authorization": token})
        python_json = python_repos.json()
        
        java_url = 'https://api.github.com/search/repositories?q=+language:Java&sort=updated&order=desc&per_page=50'
        java_repos = requests.get(java_url, headers={"Authorization": token})
        java_json = java_repos.json()
        
        ruby_url = 'https://api.github.com/search/repositories?q=+language:Ruby&sort=updated&order=desc&per_page=50'
        ruby_repos = requests.get(ruby_url, headers={"Authorization": token})
        ruby_json = ruby_repos.json()
        
       
        # Parsing json 
        if 'items' in python_json:       
            for repo in python_json['items']:
        
                if repo['description'] is None:
                    repo_data = str(repo['language']) + ';' + str(re.sub("Z","", re.sub("T"," ",repo["pushed_at"]))) +  ';' + str(repo['id']) + ';' + str(repo['full_name']) + ';' + str(repo['stargazers_count'])+ ';' + str(repo['description']) + '\n'
                    data = f"{repo_data}".encode()
                    conn.send(data)
                    print(repo_data)
                    
                else:
                    repo_data = str(repo['language']) + ';' + str(re.sub("Z","", re.sub("T"," ",repo["pushed_at"])))  +  ';' + str(repo['id']) + ';' + str(repo['full_name']) + ';' + str(repo['stargazers_count'])+ ';' + str(re.sub('[^a-zA-Z ]', '', str(repo['description']))) + '\n'
                    data = f"{repo_data}".encode()
                    conn.send(data)
                    print(repo_data)
        
        if 'items' in java_json:
            for repo in java_json['items']:
                
                if repo['description'] is None:
                    repo_data = str(repo['language']) + ';' + str(re.sub("Z","", re.sub("T"," ",repo["pushed_at"]))) +  ';' + str(repo['id']) + ';' + str(repo['full_name']) + ';' + str(repo['stargazers_count'])+ ';' + str(repo['description']) + '\n'
                    data = f"{repo_data}".encode()
                    conn.send(data)
                    print(repo_data)
                    
                else:
                    repo_data = str(repo['language']) + ';' + str(re.sub("Z","", re.sub("T"," ",repo["pushed_at"])))  +  ';' + str(repo['id']) + ';' + str(repo['full_name']) + ';' + str(repo['stargazers_count'])+ ';' + str(re.sub('[^a-zA-Z ]', '', str(repo['description']))) + '\n'
                    data = f"{repo_data}".encode()
                    conn.send(data)
                    print(repo_data)
                    
        if 'items' in ruby_json:   
            for repo in ruby_json['items']:
                
                
                if repo['description'] is None:
                    repo_data = str(repo['language']) + ';' + str(re.sub("Z","", re.sub("T"," ",repo["pushed_at"])))  +  ';' + str(repo['id']) + ';' + str(repo['full_name']) + ';' + str(repo['stargazers_count'])+ ';' + str(repo['description']) + '\n'
                    data = f"{repo_data}".encode()
                    conn.send(data)
                    print(repo_data)
                    
                else:
                    repo_data = str(repo['language']) + ';' + str(re.sub("Z","", re.sub("T"," ",repo["pushed_at"])))  +  ';' + str(repo['id']) + ';' + str(repo['full_name']) + ';' + str(repo['stargazers_count'])+ ';' + str(re.sub('[^a-zA-Z ]', '', str(repo['description']))) + '\n'
                    data = f"{repo_data}".encode()
                    conn.send(data)
                    print(repo_data)
                
        time.sleep(15)
                
        
        
        
    except KeyboardInterrupt:
        s.shutdown(socket.SHUT_RD)
        