import spacy
from spacy import displacy
import json
import pymysql

from relationExtractor import parser, extractOneAndMany
from utils import getBestMatch,checkDepPatterns,checkOrdinal
import config
from spacy.matcher import Matcher
##  ********* Database Connection *********
database="college"
connection = pymysql.connect(host=config.hostname, user=config.user, passwd=config.passwd, database=database)
cursor = connection.cursor()
##  ********* End Database Connection *********

with open('mapping.json', 'r') as fp:
    mapping = json.load(fp)
tables, tables_attributes, tables_pk, tables_relation = parser(
    'college.sql.txt')  # Later load from files
# print(tables,tables_attributes,tables_pk,tables_relation,sep='\n')
# print("Tables: ",tables,sep='\n')
# print()
# print("Table with attributes: ",tables_attributes,sep='\n')
# print()
# print("Table PK: ",tables_pk,sep='\n')
# print()
# print("Table Relations: ",tables_relation,sep='\n')
ManyToOne, ManyToMany, exclude = extractOneAndMany(tables_relation, tables_pk)
# print("ManyToOne: ",ManyToOne)
# print("ManyToMany: ",ManyToMany)
# print("exclude: ",exclude)

nlp = spacy.load('en_core_web_sm')
matcher = Matcher(nlp.vocab)
result=""


pattern1a=[]
pattern2a=[]
pattern3a=[]
pattern1a.append([{"POS": "CCONJ","OP":"*"},{"POS": {"IN":["NOUN","PROPN"]}}, {"POS": "AUX"}, {"POS": {"IN":["NOUN","PROPN","NUM"]}}])
pattern2a.append([{"POS": "CCONJ","OP":"*"},{"POS":{"IN":["AUX","ADP"]},"OP":"*"},{"POS":{"IN":["NOUN","PROPN","NUM"]}},{"POS":{"IN":["NOUN","PROPN","NUM"]}}])
pattern3a.append([{"POS": "CCONJ","OP":"*"},{"POS": {"IN":["NOUN","PROPN"]}}, {"POS": "AUX","OP":"*"}, {"POS": "ADJ"},{"POS":"SCONJ"}, {"POS": "DET","OP":"*"},{"POS":{"IN":["NOUN","PROPN","NUM"]}}])





matcher.add("three_and",pattern1a)
matcher.add("two_and",pattern2a)
matcher.add("others_and",pattern3a)
def special_condition(doc):
    if(doc[1].pos_=="PROPN" and len([data for data in doc[1].children])==1):
        return True
    return False

def conjunction(doc):
    res=[]
    res.append(str(doc[0]).upper())
    res.append(doc[1:])
    return res

def p1(doc,table):
    result=""
    if(doc[0].pos_=="CCONJ"):
        result+=conjunction(doc)[0]+" "
        doc=conjunction(doc)[1]

    for data in table:
        tname=getBestMatch(data,tables)
        #print("TABLE",tname[0])
        attr=getBestMatch(str(doc[0]),tables_attributes[tname[0]])
        #print("ATTR",attr,tables_attributes[tname[0]],str(doc[0]))
        if(attr!=None):    
            result+=tname[0]+"."+str(attr[0])+"='"+str(doc[2])+"' "
            break
    
    return result 

def p2(doc,table):
    result=""
    tname=""
    value=None
    attr_unmapped=None
    if(doc[0].pos_=="CCONJ"):
        result+=conjunction(doc)[0]+" "
        doc=conjunction(doc)[1]
    if len(doc)>2:
        doc=doc[len(doc)-2:len(doc)]

    print("Inside Two word chunk",str(doc[0]),str(doc[1]))  
    if(special_condition(doc)):
        value=doc[1]
        attr_unmapped=doc[0] 
    else:    
        if(len([data for data in doc[0].children])==0):
            value=doc[0]
            attr_unmapped=doc[1]
        else:
            value=doc[1]  
            attr_unmapped=doc[0]  
    
    dist=100000
    attribute=""
    for data in table:
        attr=getBestMatch(attr_unmapped.text,tables_attributes[table[data]])
        if attr!=None:
            if attr[1]<dist:
                tname=table[data]
                attribute=attr[0]
                dist=attr[1]


    """ print("MAIN",value.text)
    for ancestor in value.ancestors:
        if(ancestor.pos_=="NOUN" or ancestor.pos_=="PROPN"):
            attr_unmapped=ancestor
            break
    for ancestor in attr_unmapped.ancestors:
        if(ancestor.text in table):
            tname=table[ancestor.text]
            break


        print(attr_unmapped.text,ancestor)    
    print("#"*40)
    print(tname,value.text,attr_unmapped.text) """
    """ attribute=predictAttr(tname,value.text)
    result+=tname+"."+str(attribute)+"='"+str(value)+"' " """
    if(tname!="" and attribute!=""):
        result+=tname+"."+str(attribute)+"='"+str(value.text)+"' "
    
            
    





            
    
    return result 

def p3(doc,table):
    if(doc[0].pos_=="CONJ"):
        result+=conjunction(doc)[0]+" "
        doc=conjunction(doc)[1]   
    #remove det and aux
    result="" 
    flag=True   
    for data in doc:
        if(data.pos_=="AUX" or data.pos_=="DET"):
            continue
        elif(data.pos_=="NOUN" and flag):
            flag=False
            for t in table:
                tname=getBestMatch(t,tables)
                attr=getBestMatch(str(data),tables_attributes[tname[0]])        
                if(attr!=None):
                    result+=str(tname[0]+"."+attr[0])+" "
                    break
        else:
            result+= str(data)+" "
    for k in mapping.keys():
        for data in mapping[k]:
            if result.find(data)!=-1:
                result=result.replace(data,k)
    return result                        



    




def findPath(ManyToMany, start_table, end_table):
    visited = set()
    visited.add(start_table)
    queue = [[start_table, []]]
    while len(queue) != 0:
        # node with current-table and list of intermediate tables
        current = queue.pop(0)
        for key, value in ManyToMany.items():
            if current[0] in value:
                other_table = [table for table in value if table != current[0]]
                if end_table in other_table:
                    path = current[1].copy()
                    path.append(key)
                    return path
                for new_current in other_table:
                    path = current[1].copy()
                    path.append(key)
                    queue.append([new_current, path])
    return None


def map_to_tables(possible_tables, possible_attributes):
    tables_involved = {}
    for word in possible_tables:
        # print(type(word),type(word.text))
        best_match = getBestMatch(word.lemma_, tables, 2)
        # print(best_match)
        if best_match == None:
            possible_attributes.add(word)
        else:
            tables_involved[word] = best_match[0]
    return tables_involved


def getManyToOneAttr(t1, t2, ManyToOne):
    for relation in ManyToOne:
        if t1 in relation and t2 in relation:
            return relation[2]
    return None


def getManyToManyAttr(t1, t2, ManyToMany):
    for joining_table, tables in ManyToMany.items():
        if t1 in tables and t2 in tables:
            return joining_table
    return None


def generate_join_condition(table_path):
    join_condition = []
    for i in range(1, len(table_path)):
        t1 = table_path[i-1]
        t2 = table_path[i]
        # print("Join Condition for: ",t1,t2)
        common_attr = list(set(tables_pk[t1]).intersection(tables_pk[t2]))
        # print(common_attr)
        if len(common_attr) == 1:
            common_attr = common_attr[0]
            join_condition.append(f"{t1}.{common_attr}={t2}.{common_attr}")
    return join_condition


def getJoinedTablesData(dependent_on, ManyToOne, ManyToMany):
    extra_tables = []
    join_on = []
    for child_table, parent_table in dependent_on.items():
        attr = getManyToOneAttr(child_table, parent_table, ManyToOne)
        if attr != None:
            join_on.append(f"{child_table}.{attr}={parent_table}.{attr}")
        else:
            joining_table = getManyToManyAttr(
                child_table, parent_table, ManyToMany)
            if joining_table != None:
                extra_tables.append(joining_table)
                # print("Table Path: ",[child_table,joining_table,parent_table])
                join_on = join_on + \
                    generate_join_condition(
                        [child_table, joining_table, parent_table])
                # Many to many
            else:
                table_path = findPath(ManyToMany, child_table, parent_table)
                if table_path != None:
                    extra_tables = extra_tables+table_path
                    join_path = [child_table]
                    for t in table_path:
                        join_path.append(t)
                    join_path.append(parent_table)
                    join_on = join_on + generate_join_condition(join_path)
                # Path search remaining
    return extra_tables, join_on


def get_table_query(tables_involved, ManyToOne, ManyToMany):
    table_query = None
    if len(tables_involved) < 2:
        for k, v in tables_involved.items():
            table_query = v
        return table_query
    dependent_on = {}
    if len(tables_involved) == 2:
        tables = list(tables_involved.keys())
        dependent_on[tables_involved[tables[0]]] = tables_involved[tables[1]]
    else:
        for table_token, table_str in tables_involved.items():
            for ancestor in table_token.ancestors:
                if ancestor in tables_involved:
                    dependent_on[table_str] = tables_involved[ancestor]
                    break
    print("Table Dependency extracted: ",dependent_on,sep='\n')
    print()
    extra_tables, join_on = getJoinedTablesData(
        dependent_on, ManyToOne, ManyToMany)
    tables_join = " INNER JOIN ".join(
        list(tables_involved.values())+extra_tables)
    join_on = " AND ".join(join_on)
    table_query = tables_join+" ON "+join_on
    return table_query


def checkAggregate(word):
    for k, v in mapping.items():
        if word in v:
            return k
    return None


def attr_to_parent_table(attributes, tables_involved):
    table = set(attributes).intersection(set(tables_involved.keys()))
    if table:
        return [], list(table)[0]
    for attribute in attributes:
        attrs = []
        table = None
        for attr in attribute.subtree:
            if attr in tables_involved:
                table = attr
                break
            if attr.pos_ == "NOUN" or attr.pos_ == "PROPN":
                attrs.append(attr)
        if table:
            return attrs, table
    return None


def get_select_attr(tables_involved, root_token):
    # parent_table = list(find_parent(tables_involved))[0]
    attributes = set()
    for child in root_token.children:
        if child.dep_ in ["nsubj", "dobj", "ccomp"]:
            attributes.add(child)
    print("Attributes: ", attributes,sep='\n')
    print()
    attributes, parent_table = attr_to_parent_table(
        attributes, tables_involved)
    print("Finalised Attributes: ",sep='\n')
    print(attributes, parent_table)
    print()
    query = "*"
    # print("Parent table: ",parent_table)
    # print("Tables_involved: ",tables_involved)
    parent_table_map = tables_involved[parent_table]
    if len(attributes) == 0:    # Example: List the students
        aggDep = None
        for child in parent_table.children:   # Check for any aggregates , Find number of students
            if child.dep_ == "amod":
                aggDep = child
        if aggDep and checkAggregate(aggDep.text):
            agg = checkAggregate(aggDep.text)
            query = f"{agg}(*)"
        else:
            # if len(tables_involved)==1:   # If single table just put *
            #     query="*"
            # else:                         # If multiple tables transform to table_name.attr_name
            #   select_attrs=[]
            #   for att in tables_attributes[parent_table_map]:
            #     select_attrs.append(f"{parent_table_map}.{att}")
            #   query=', '.join(select_attrs)
            query = "*"
    elif len(attributes) == 1:    # maximum age
        selected_attribute = attributes[0]
        # Check the word itself is not an aggregate
        agg = checkAggregate(selected_attribute.text)
        if agg:
            query = agg+"(*)"
        else:
            aggDep = None
            for child in selected_attribute.children:
                if child.dep_ == "amod":
                    aggDep = child
            selected_attribute = getBestMatch(
                selected_attribute.text, tables_attributes[parent_table_map], 7.5)[0]
            if aggDep and checkAggregate(aggDep.text):
                agg = checkAggregate(aggDep.text)
                query = f"{agg}({selected_attribute})"
            else:
                query = f"{parent_table_map}.{selected_attribute}"
    else:
        selected_attribute = attributes[0]
        agg = checkAggregate(selected_attribute.text)
        # Mapping to be done
        if agg:
            try:
                attr = getBestMatch(
                    attributes[1].text, tables_attributes[parent_table_map], 7.5)[0]
                query = f"{agg}({attr})"

            except:
                query = f"{agg}({attributes[1].text})"
        else:
            mapped_attributes = []
            for attribute in attributes:
                try:
                    mapped_attributes.append(getBestMatch(
                        attribute.text, tables_attributes[parent_table_map], 7.5)[0])
                except:
                    pass
            selected_attribute = [parent_table_map+"." +
                                  attribute for attribute in mapped_attributes]
            query = ",".join(selected_attribute)
        # print("Final")   multiple attributes left
    # print(attributes)
    return query

def predictAttr(table_name,value):
    for attribute in tables_attributes[table_name]:
        query=f"SELECT COUNT(*) FROM {table_name} WHERE {attribute} LIKE '{value}'"
        try:
            cursor.execute(query)
            val=cursor.fetchone()[0]
            if val!=0:
                return attribute
        except:
            pass
    return 'name'

def uFacCondition(conditions,tables_involved):
    # print(tables_involved)
    uFacConditions=[]
    for condition in conditions:
        try:
            table_name=condition['table_name']
            value=condition['value']
            predictedAttr=predictAttr(table_name=tables_involved[table_name],value=value)
            # print("Attribute Predicted :*/*/*/*/ ",predictedAttr)
            uFacConditions.append(f"{tables_involved[table_name]}.{predictedAttr}='{value}'")
        except:
            # possibleAttr=condition['table_name']
            # value=condition['value']
            # if checkOrdinal(value):
            #     value=value[:len(value)-2]
            # list_of_attrs=[]
            # for table in tables_involved.values():
            #     list_of_attrs.extend(tables_attributes[table])
            # attr=getBestMatch(possibleAttr,list_of_attrs,7.5)[0]
            # table_name=None
            # for table in tables_involved.values():
            #     if attr in tables_attributes[table]:
            #         table_name=table
            #         break
            # uFacConditions.append(f"{table_name}.{attr}='{value}'")
            pass
    if len(uFacConditions)==0:
        return None
    return {" AND ":uFacConditions}

def formPatternConditions(patternMatch,tables_involved):
    patternConditions=[]
    for patternType,conditions in patternMatch.items():
        if patternType=="uFacPattern":
            uFacConditions=uFacCondition(conditions,tables_involved)
            if uFacConditions:
                patternConditions.append(uFacConditions)
        else:
            pass   # For other patterns
    return patternConditions

def repeated_data(matches):
    matches.sort(key=lambda x: x[1])
    data=[]
    current_end=0
    for id,start,end in matches:
        if current_end==end:
            continue
        else:
            data.append([id,start,end])
            current_end=end
    return data                

def generate_where(matches,doc,tables_involved):
    res=""
    print("MAtches before:",matches,sep='\n')
    matches=repeated_data(matches)
    print("Matches After:",matches,sep='\n')
    print()
    for data in matches:
        start=data[1]
        end=data[2]
        string_id = nlp.vocab.strings[data[0]]
        # print("String ID",string_id)
        if(string_id=="three_and"):
            res+=p1(doc[start:end],tables_involved)
        elif(string_id=="two_and"):
            res+=p2(doc[start:end],tables_involved)
        elif(string_id=="others_and"):
            res+=p3(doc[start:end],tables_involved)
    print("Chunk Conditions: ",res,sep='\n') 
    print()
    return res              



def constructWhereCondition(doc,tables_involved):
    patternMatch=checkDepPatterns(doc)
    patternConditions=[]
    if len(patternMatch)!=0:
        patternConditions=formPatternConditions(patternMatch,tables_involved)
        print("Pattern Conditions Extracted:",patternConditions,sep='\n')
        print()
    # print("Inside where")
    matches=matcher(doc)
    
       
    return join_dama_dev([generate_where(matches,doc,tables_involved),patternConditions])

def join_dama_dev(l):
    result=""
    if(l[0]==""):
        for data in l[1]:
            for k,v in data.items():
                if(len(v)==1):
                    result+=v[0]
                else:
                    result+=k.join(v)    
    else:
        if(len(l[1])==0):
            result+=l[0]
        else:
            result+=l[0]+"AND "
            for data in l[1]:
                for k,v in data.items():
                    if(len(v)==1):
                        result+=v[0]
                    else:
                        result+=k.join(v) 
    return result            

def getsql(sample_text):
    global database
    # sample_text=sample_text.replace(',',' ')
    map_schema = {}

    agg = ''
    doc = nlp(sample_text)
    possible_tables = set()
    possible_attributes = set()
    for word in doc:
        if word.pos_ == "NOUN":
            possible_tables.add(word)
        elif word.pos_ == "PROPN" or word.pos_ == "NUM":
            possible_attributes.add(word)
    print("*** Table Extraction ***")
    print("Possible tables: ", possible_tables,sep='\n')
    print()
    tables_involved = map_to_tables(possible_tables, possible_attributes)
    
    table_query = get_table_query(tables_involved, ManyToOne, ManyToMany)
    print("Table Query:",table_query,sep='\n')
    print()
    print("*** Select and Aggregate ***")
    select_attr_query = get_select_attr(
        tables_involved, list(doc.sents)[0].root)
    print("Selected Attributes: ",select_attr_query,sep='\n')
    print()

    tables_involved_str={}   # String key mandatory for this islie kiya
    for k,v in tables_involved.items():
        tables_involved_str[k.text]=v
    # print(tables_involved)
    print("*** WHERE conditions ***")
    where_query=constructWhereCondition(doc,tables_involved_str)
    if(where_query==""):
        where_query="1"
    print("Final where Condition: ",where_query,sep='\n')
    print()
    print("*** Final Query ***")
    resultant=f"SELECT {select_attr_query} FROM {table_query} WHERE {where_query}"
    print("SQL Query: ",resultant)
    # for k,v in tables_involved.items():
    #   for ancestor in k.ancestors:
    #     print(k,ancestor)
    # print("Updated Possible attributes: ", possible_attributes)


# sample_text=input("Query: ")
examples = [
    # "Find the name,gender of student with age greater than 20 and in semester 5",
    #  "How many students have age greater than 20 and are in semester 5",
    # "In Computer department, find the number of male students who are in semester 7 ",
    #  "Find the faculties who have joined in year 2008",
    #  "How many students in Computer department are under the age of 20",
    # # # khahdj
    #  "List the students who have opted for NLP subject",
    #  "List the maximum age of students in each department",
    #  "List the faculties teaching students of Computer department in 8th semester",
    #  "Find out the male students in Computer department?",
    #  "What is the maximum age of male student whose department is Computer?",
    # # #
    #  "Find the students belonging to Computer department and under the age of 20",
    #  "Find the students whose department is Computer",
    #  "Get the name of the students whose rollno are 1711008, 1711007 or 9",
    #  "Find students in Computer department who have NLP as a subject",
    # #  "Find the sum of marks of all students",
    # #  "How many students have their rollno starting as '1711' ?",
    #  "What is the name of the student with rollno 1711552 and age less than 20?",
    #  "What is Rollcall, semester of the student whose name is Sagar and above 20 years old",
    #  "Find the name,gender of students whose rollno is 20 and name is Sagar and age is less than 30",
    #  "Find the number of male students who are in semester 7 ",
    #  "Find the subjects taught by Computer department along with their corresponding faculties",
    #  "What is the name of the female student with Rollno 1711152",
    #  "Find the data of student with name Sagar",
    "Find the subjects taught by Computer department along with their corresponding faculties"
]
for example in examples:
    print("Q: ", example)
#   displacy.render(nlp(example), style='dep', jupyter=True,options={'distance': 100})
    getsql(example)
    print("*"*100, '\n')
# getsql(sample_text)
# displacy.render(nlp(sample_text), style='dep',jupyter=False,options={'distance': 100})
