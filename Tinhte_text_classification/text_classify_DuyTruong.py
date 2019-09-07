import os 
import pandas as pd 
from sklearn.feature_extraction.text import CountVectorizer
os.chdir('/home/duytruong/Working/Tinhte_question_classification/text_classification')

comment_raw = pd.read_csv("dataset_1.csv",usecols=['post_id','message','is_question'],encoding='utf-8')
comment_raw = comment_raw.dropna(subset=['message'])
comment_raw.head()

# Remove duplicate comment 
comment_raw = comment_raw.loc[~comment_raw[['message', 'is_question']].duplicated(), :]


vn_stop = [line.strip() for line in open("vietnamese.txt")]


vect = CountVectorizer(stop_words=vn_stop, max_df = 0.5, min_df = 0.001, ngram_range = (1, 5), lowercase=True)
vect.fit(comment_raw.message)


vocabulary_df = pd.DataFrame({
        'word': list(vect.vocabulary_.keys()),
        'fequent': list(vect.vocabulary_.values())
        })

# remove bbcode tag number 
def number_cleaner(text):
    return ' '.join([i if not i.isnumeric() else '' for i in text.split(' ')])

def remove_bbcode_tags(text):
    """Remove html tags from a string"""
    import re
    clean = re.compile('\[.*?]')
    return re.sub(clean, '', text)

def remove_punctuation(text):
    import string
    exclude = set(string.punctuation) 
    return ''.join([i if i not in exclude else '' for i in text])

def find_question_mark(text):
    return 1 if '?' in text else 0
    

comment_raw['no_bbcode_message'] = comment_raw.message.apply(remove_bbcode_tags)   
comment_raw['removed_punc_message'] = comment_raw.no_bbcode_message.apply(remove_punctuation)           
comment_raw['clean_message'] = comment_raw.removed_punc_message.apply(number_cleaner)       

vect.fit(comment_raw.message)
vocabulary_df = pd.DataFrame({
        'word': list(vect.vocabulary_.keys()),
        'fequent': list(vect.vocabulary_.values())
        })

# find keywords in question 
vect.fit(comment_raw.message[comment_raw.is_question == 1])
vocabulary_df = pd.DataFrame({
        'word': list(vect.vocabulary_.keys()),
        'fequent': list(vect.vocabulary_.values())
        })
    
# Train test split
from sklearn.cross_validation import train_test_split
#x = comment_raw.message
#y = comment_raw.is_question
train_df, test_df = train_test_split(comment_raw,test_size=0.2,random_state=1)


# Train model 
from sklearn.naive_bayes import MultinomialNB
import scipy.sparse as ss
from sklearn.ensemble import RandomForestClassifier
rfc = RandomForestClassifier()
NB = MultinomialNB()
x_train_dtm = vect.fit_transform(train_df.message)
x_train_qm = train_df.message.apply(find_question_mark).values
x_train_dtm = ss.hstack([x_train_dtm, ss.csr_matrix(x_train_qm).T])
y_train = train_df.is_question
NB.fit(x_train_dtm,y_train)
rfc.fit(x_train_dtm,y_train)
# Test model

x_test_dtm = vect.transform(test_df.message)
x_test_qm = test_df.message.apply(find_question_mark).values
x_test_dtm = ss.hstack([x_test_dtm, ss.csr_matrix(x_test_qm).T])
t_test =  test_df.is_question
y_predict = NB.predict(x_test_dtm)
y_predict_2 = rfc.predict(x_test_dtm)

from sklearn.metrics import accuracy_score, precision_score, recall_score
print(accuracy_score(y_test, y_predict))
print(precision_score(y_test, y_predict))
print(recall_score(y_test, y_predict))

print(accuracy_score(y_test, y_predict_2))
print(precision_score(y_test, y_predict_2))
print(recall_score(y_test, y_predict_2))



parameters = {
        'n_estimators' : [5, 10, 15, 20, 25, 30],
        'criterion' : ['gini', 'entropy'],
        'max_features' : [100, 120, 140],
        'max_depth' : range(15, 30, 2),
        'min_samples_split' : range(10, 20),
        'min_samples_leaf' : range(1, 10),
        'max_leaf_nodes' :  range(10, 25)
        }

from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
rfc = RandomForestClassifier()
gscv = RandomizedSearchCV(rfc, parameters, n_iter = 500, cv=3, scoring = 'f1', n_jobs = 2, verbose = 3)
gscv.fit(x_train_dtm,y_train)
y_predict = gscv.predict(x_test_dtm)

print(accuracy_score(y_test, y_predict))
print(precision_score(y_test, y_predict))
print(recall_score(y_test, y_predict))



















    