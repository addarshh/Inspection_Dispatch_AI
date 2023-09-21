from base64 import encode
from camel_tools.utils.normalize import normalize_alef_maksura_ar, normalize_alef_ar, normalize_teh_marbuta_ar, normalize_unicode
from collections import Counter
import pandas as pd
import gensim
import nltk
from nltk.corpus import stopwords
from gensim.utils import simple_preprocess

def isArabicAlpha(string, includeWhiteSpace=False):
    #Return True if all characters in the string are arabic characters. Return False otherwise. set includeWhiteSpace to True if keeping space and newline is needed.
    arabicChars = {'ا', 'آ', 'أ', 'إ', 'ب', 'ت', 'ث', 'ج', 'ح', 'خ', 'د', 'ذ', 'ر', 'ز', 'س', 'ش', 'ص', 'ض', 'ط', 'ظ', 'ع', 'غ', 'ف', 'ق', 'ك', 'ل', 'م', 'ن', 'ه', 'و', 'ي', 'ة', 'ؤ', 'ى', 'ئ', 'ء', }
    if includeWhiteSpace:
        arabicChars.update([' ', '\n'])
    if len(string) == 0:
        return False
    else:
        for char in string:
            if char not in arabicChars:
                return False
        return True

def removeNonArabicSymbols(string):
    #Removes all non arabic symbols but keeps space and newline characters
    return ''.join(filter(lambda s: isArabicAlpha(s, includeWhiteSpace=True), string))

def arabic_normalization(text):
    #takes in text (one string) and returns the normalized text (one string)

    sent_norm = normalize_unicode(text)
    
    # Normalize alef variants to 'ا'
    sent_norm = normalize_alef_ar(sent_norm)

    # Normalize alef maksura 'ى' to yeh 'ي'
    sent_norm = normalize_alef_maksura_ar(sent_norm)

    # Normalize teh marbuta 'ة' to heh 'ه'
    sent_norm = normalize_teh_marbuta_ar(sent_norm)

    return sent_norm

def sent_to_words(sentences):
    for sentence in sentences:
        yield(gensim.utils.simple_preprocess(str(sentence), deacc=True))

# Define functions for stopwords
def remove_stopwords(texts, VP_Label):
    #nltk.download('stopwords')
    #Read stopwords input config
    df_stopwords = pd.read_csv(r"C:\MOMRA\AllStopwords.csv", dtype=object)
    #Read stopwords relative only to current VP label or general (applies to all VPs)
    vp_stop = df_stopwords[(df_stopwords['VP_Label']==VP_Label) | (df_stopwords['VP_Label']=='General')].reset_index().iloc[: , 1:]
    stop = []
    for i in range (0, vp_stop.shape[0]):
        #Append only non-important words as stopwords
        if str(vp_stop['is Important? (Yes, no)'][i]).lower() == 'no':
            stop.append(vp_stop['Word'][i])
    #Include arabic stopwords from nltk library
    stop_words = stopwords.words('arabic')
    dataframe = pd.DataFrame(stop_words, columns = ['word'])
    #Normalize arabic stopwords from nltk library to also include as stopwords
    dataframe['word'] = dataframe['word'].apply(arabic_normalization)
    #convert to list
    stop_words.extend(dataframe['word'].tolist())
    #remove stop words defined by business
    stop_words.extend(stop)
    return [[word for word in simple_preprocess(str(doc)) if word not in stop_words] for doc in texts]