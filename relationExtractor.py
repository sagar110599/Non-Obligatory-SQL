from nltk.stem import WordNetLemmatizer
import re
import sys
from nltk.tokenize import word_tokenize

import re

class my_dictionary(dict): 

    # __init__ function 
    def __init__(self): 
        self = dict() 
        
    # Function to add key:value 
    def add(self, key, value): 
        self[key] = value 

def parser(database='college.sql'):
    tables=[]
    tables_attributes={}
    tables_pk=my_dictionary()
    tables_relation=[]
    def action_PK(tables_pk,table_name,line):
        y=line.split("(")[1].split(")")[0]
        #print(y+" "+table_name)
        y=y.replace("`","")
        l=y.split(",")
        tables_pk.add(table_name.replace("`",""),l)
    def action_constraint(tables_relation,table_name,line):
        y=line.split("FOREIGN KEY (")[1].split(")")[0]
        y=y.replace("`","")
        t2=line.split("REFERENCES `")[1].split("`")[0]
        # print(t2)
        # print(y)
        p=[]
        p.append(table_name.replace("`",""))
        p.append(t2)
        p.append(y)
        tables_relation.append(p)

    file1=open(database)
    lines=file1.readlines()
    # print(lines)
    s="CREATE TABLE"
    a="ALTER TABLE"
    pk="ADD PRIMARY KEY"
    constraint="ADD CONSTRAINT"
    isKey=False
    table_name=""
    for i in range(len(lines)):
        #print(line)
        if(isKey):
            if(lines[i].find(pk)!=-1):
                action_PK(tables_pk,table_name,lines[i])
            elif(lines[i].find(constraint)!=-1):
                action_constraint(tables_relation,table_name,lines[i])
            else:
                isKey=False
        else:
            x = re.findall("\A"+s+"",lines[i])
            x1 = re.findall(r'^CREATE TABLE [A-Za-z_`()0-9]*',lines[i])
            # if(x):
            #     #print(x)
            #     y=word_tokenize(str(lines[i]))
            #     tables.append(y[3])
            if(x1):
                size=len(x1[0])-1
                x1=x1[0][14:size]
                tables.append(x1)
                tables_attributes[x1]=[]
                for j in range(i+1,len(lines)):
                    if(not re.findall(r';$',lines[j])):
                        s=lines[j].split()[0]
                        s=s[1:len(s)-1]
                        tables_attributes[x1].append(s)
                    else:
                        break
            else:
                y=re.findall("\A"+a+"",lines[i])
                try:
                  if(y):
                      p=word_tokenize(str(lines[i]))
                    #   print(p)
                      table_name=p[3] # p[2] for Google Colab 
                      isKey=True 
                except:
                  print(p)
                  pass
    return tables, tables_attributes, tables_pk,tables_relation

def extractOneAndMany(tables_relation,tables_pk):
  ManyToOne=tables_relation.copy()
  ManyToManyTables=[]
  table_fk_count={}
  for relation in tables_relation:
    table_fk_count[relation[0]]=table_fk_count.get(relation[0],0)+1
  exclude = [k for k,v in table_fk_count.items() if v>1]
  # print(exclude)
  for relation in tables_relation:
    for table in exclude:
      if table in relation:
        ManyToManyTables.append(relation)
        ManyToOne.remove(relation)
        break
  ManyToMany={}
  for relation in ManyToManyTables:
    ManyToMany[relation[0]]=ManyToMany.get(relation[0],[])+[relation[1]]
  return ManyToOne, ManyToMany, exclude