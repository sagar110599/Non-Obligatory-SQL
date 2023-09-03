import spacy
from spacy.matcher import DependencyMatcher

nlp = spacy.load("en_core_web_sm")
matcher = DependencyMatcher(nlp.vocab)

uFacPattern = [
     {"SPEC": {"NODE_NAME": "table_name"}, "PATTERN": {"DEP": {"IN":["pobj","dobj"]}}},
    {"SPEC": {"NODE_NAME": "value", "NBOR_RELOP": ">", "NBOR_NAME": "table_name"}, "PATTERN": {"DEP": {"IN": ["amod", "compound"]},"POS":{"IN":["ADJ","PROPN"]}}},
]
matcher.add("uFacPattern",[uFacPattern])

def checkDepPatterns(doc):
    patternMatch={}
    matches = matcher(doc)
    for match in matches:
        match_id, token_ids = match
        match_type= nlp.vocab.strings[match_id]
        patternMatch[match_type]=[]
        count=0
        for token_id in token_ids:
            # print(token_id)
            patternMatch[match_type].append({})
            for j in range(len(token_id)):
                if match_type=="uFacPattern":
                    # print(uFacPattern[j]["SPEC"]["NODE_NAME"] + ":", doc[token_id[j]].text)
                    nodeName=uFacPattern[j]["SPEC"]["NODE_NAME"]
                    val=doc[token_id[j]].text
                    patternMatch[match_type][count][nodeName]=val
                else:
                    # print(uFacPattern2[j]["SPEC"]["NODE_NAME"] + ":", doc[token_id[j]].text)
                    pass
            count+=1
    return patternMatch 

# sample_text=input("Query: ")
# doc = nlp(sample_text)
# print(checkDepPatterns(doc))
replace_cost=2
del_cost=2
ins_cost=1
def editDistDP(str1, str2):
  m=len(str1)
  n=len(str2)
  str1=str1.lower()
  str2=str2.lower()
  dp = [[0 for x in range(n + 1)] for x in range(m + 1)]
  for i in range(m + 1):
    for j in range(n + 1):

        if i == 0:
            dp[i][j] = j*ins_cost    # Min. operations = j

        elif j == 0:
            dp[i][j] = i*del_cost    # Min. operations = i

        # If last characters are same, ignore last char
        # and recur for remaining string
        elif str1[i-1] == str2[j-1]:
            dp[i][j] = dp[i-1][j-1]

        else:
            dp[i][j] = min(ins_cost + dp[i][j-1],        # Insert
                                del_cost + dp[i-1][j],        # Remove
                                replace_cost + dp[i-1][j-1])    # Replace

  # return dp[m][n]/(1+abs(m-n))
  return dp[m][n]-0.8*abs(m-n)

  # return dp[m][n]

def getBestMatch(current_word,list_of_words,threshold=6):
  current_min_distance=100000000
  best_match=None
  for word in list_of_words:
    distance=editDistDP(current_word,word)
    if distance<=threshold and current_min_distance>distance:
      current_min_distance=distance
      best_match=[
        word,distance
      ]
  return best_match

def checkOrdinal(v):

   f=lambda v:'hsnrhhhhhh'[(v[-4:-3]!='1')*int(v[-3])]in v
   try:
       return f(v)
   except:
       return False
