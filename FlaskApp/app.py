from flask import Flask,render_template,request,url_for,redirect
import speech_recognition as sr
import sqlparser2
import nltk
import os
import pickle
import json
import time 
import spacy
from spacy import displacy
from relationExtractor import parser, extractOneAndMany
from utils import getBestMatch,checkDepPatterns,checkOrdinal
from spacy.matcher import Matcher
import pymysql
import database_config
from nltk.stem import WordNetLemmatizer 
import csv
import time
import threading
from QueryMaker import Construct
from queries import query
app = Flask(__name__)

app.config['File_uploads']=database_config.file_path+"sql_metadata"
database = ''
dic1 = {}
csv_list = []
@app.route('/')
def home():
    return render_template("start.html")

@app.route('/panel')
@app.route('/panel', methods=['POST']) 
def hello():
    
    if request.method == "POST":
        uploaded_file = request.files['userfile']
        directory=os.path.join(app.config['File_uploads'],uploaded_file.filename.split(".")[0])
        datafile=uploaded_file.filename.split(".")[0]
        print(directory)
        print(datafile)
        if uploaded_file.filename.split(".")[1]=="sql":
            if(os.path.isdir(directory)==False):
                os.mkdir(directory)
                uploaded_file.save(os.path.join(directory,uploaded_file.filename))
                sqlparser2.parser(datafile)
                print(uploaded_file.filename)
    data={}
    for file in os.listdir(app.config['File_uploads']):
        data[file]={}
        table_attr_path=os.path.join(app.config['File_uploads'],file+"\\table_attributes.json") 
        with open(table_attr_path) as f:
            data[file]['table']=json.load(f) 
        default_attr_path=os.path.join(app.config['File_uploads'],file+"\\table_default_attributes.json")
        #if there is default attr file we have to show them the previous selected data.
        if(os.path.isfile(default_attr_path)):
            print("default is available")
            with open(default_attr_path) as fu:
                data[file]['default']=json.load(fu)
                # print(data[file]['default'])
        else:
            data[file]['default']={}
                    



    print("*"*30, data)
    return render_template("setting.html",data=data)
    # return redirect(url_for('home'))	
@app.route("/update-default-attrs",methods=['POST'])
def update_default_attributes():
    output = request.form['res']
    output = json.loads(output)
    for k,v in output.items():
        with open('sql_metadata/'+k+'/table_default_attributes.json', 'w') as f:
            json.dump(v, f)
    return "Success"

@app.route("/bot")
def get_bot():
    global dic1
    l=[]
    i = 1
    for file in os.listdir(app.config['File_uploads']):
        l.append(file)
        dic1[str(i)]=file
        i+=1
    return render_template('home.html',file_list=l)


@app.route("/get_database",methods=['POST'])
def set_database():
    global database
    global dic1
    print(dic1)
    sample_text = request.form['post_id']
    database = dic1[sample_text]
    print(sample_text)
    sql="Database loaded"
    return '<span> ' + sql +'<br>'+database+ ' </span>'    

@app.route("/record",methods=['POST'])
def record():
    re = request.form['rec']
    if re == 'Start Recording: ':
        r = sr.Recognizer()
        with sr.Microphone() as source:
            print('Speak anything: ')
            audio = r.listen(source)
            try:
                text = r.recognize_google(audio)
                print('\nYou said: {}\n'.format(text))
            except:
                text = 'Sorry could not recognize your Voice. Speak Something'
    return text 

""" @app.route("/download",methods=['POST'])
def download():
    global csv_list
    re = request.form['download']
    if re == 'CSV downloading: ':
        with open(database_config.file_path+'static\\output.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in csv_list[-1].get():
                csvwriter.writerow(row)
    #csv_list = []
    sweet.analysis() """
    

@app.route("/getsql",methods=["POST"])
def getsql():
    global database
    sample_text = request.form['post_id']
    # sample_text=sample_text.replace(',',' ')
    # print('sample',sample_text)

    #
    result=""
    with open('sql_metadata/'+database+'/table_attributes.json') as f:
        table_attributes = json.load(f)
    with open('sql_metadata/'+database+'/table_default_attributes.json') as f:
        table_default_attributes = json.load(f)
    with open('static/mapping.json') as f:
        mapping = json.load(f)
    print(table_default_attributes) 
    with open('sql_metadata/'+database+'/tables_pk.json') as f:
        tables_pk = json.load(f)
    # print(tables_pk)
    tables= pickle.load( open( "sql_metadata/"+database+"/tables.p", "rb" ) )
    # print(tables)
    tables_relation= pickle.load( open( "sql_metadata/"+database+"/tables_relation.p", "rb" ) )
    # print(tables_relation)
    connection = pymysql.connect(host=database_config.hostname, user=database_config.user, passwd=database_config.passwd, database=database)
    cursor = connection.cursor()
    ManyToOne, ManyToMany, exclude = extractOneAndMany(tables_relation, tables_pk)
    q=Construct(tables=tables,tables_relation=tables_relation,tables_pk=tables_pk,tables_attributes=table_attributes,tables_default_attributes=table_default_attributes,cursor=cursor)
    translate_start=time.time()
    sql=q.getsql(sample_text=sample_text,ManyToMany=ManyToMany,ManyToOne=ManyToOne,exclude=exclude)
    translate_end=time.time()
    translation_time=translate_end-translate_start


    try:
        print(database)
        connection = pymysql.connect(host=database_config.hostname, user=database_config.user, passwd=database_config.passwd, database=database)
        cursor = connection.cursor()
        cursor.execute(sql+" LIMIT 5")
        rows=cursor.fetchall()
        print(rows)
        global csv_list
        col_name = []
        csv_list2 = []
        for c1 in cursor.description:
            col_name.append(c1[0])
        csv_list2.append(col_name)
        result+="<br><table style='border: 1px black; background-color: white; opacity:0.8;'>"
        for l in col_name:
            result+="<th style='border: 1px black; color: black;'>"+l+"</th>"

        for row in rows:
            csv_list2.append(row)

        count = 0
        for row in rows:
            if count <= 3:
                result+="<tr style='border: 1px black;'>"    
                for col in row:
                    result+="<td style='border: 1px black; color: black;'>"+str(col)+"</td>"
                result+="</tr>"
            count += 1 
        result+="</table><br>"
        csv_list.append(query(csv_list2))
        if(len(rows)>3 or len(cursor.description)>5):
            result+='<form method="POST" action="/display" target="_blank">'
            result+='<input type="hidden" name="query" value="'+sql+'">'
            result+="<button type='submit'><i class='fa fa-info-circle' aria-hidden='true'></i></button></form>"
        
        # print(csv_list)(swal('+sample_text+'<br>'+sql+'))

    except Exception as e:
        print(e)
        result="Try Again"
    finally:
        connection.close() 
    return ('<span>Query Details :<button value="<strong>English Query</strong>:'+sample_text+'<br><br> <strong>Sql Query</strong>: '+sql+'<br><br> <strong>Translation Time</strong>: '+str(translation_time)+' seconds" onclick="output(this)"><i class="fa fa-external-link" aria-hidden="true"></i></button></span> '+result)


@app.route("/display",methods=["POST"])
def display():
    query = request.form['query']
    try:
        print(database)
        connection = pymysql.connect(host=database_config.hostname, user=database_config.user, passwd=database_config.passwd, database=database)
        cursor = connection.cursor()
        cursor.execute(query)
    except:
        return "Sorry the Query was Invalid"
    finally:
        connection.close() 
    return render_template('display.html',cursor=cursor)

if __name__ == '__main__':
    app.run(debug=True)