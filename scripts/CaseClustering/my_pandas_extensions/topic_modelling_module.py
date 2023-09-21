#import functions from files
from my_pandas_extensions.database_util import collect_data, export_output, write_engines_output_to_database
from my_pandas_extensions.cleantext import removeNonArabicSymbols, arabic_normalization, sent_to_words, remove_stopwords
from my_pandas_extensions.print import print_output, print_word_freq
from my_pandas_extensions.build_model import compute_coherence_values
#import libraries
import cx_Oracle
import pandas as pd
import re
import gensim.corpora as corpora
import math
# Enable logging for gensim - optional
import logging
import warnings


warnings.filterwarnings("ignore",category=DeprecationWarning)
# logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# Locate the Oracle client DLLs to establish database connectivity
cx_Oracle.init_oracle_client(lib_dir=os.path.join('/var', 'www', 'html', 'docker', 'python', 'drivers', 'lib'))

#Execute run_topic_modelling main method
def run_topic_modelling(MunicipalityId, VP_Label, Start_Time=None, SpclId='auto', Past_Months=6, Max_Number_Of_Topics=10):
    #logging.info(f"running {VP_Label}")
    #Check whether the Special Classification field, Max_Number_Of_Topics, and  Past_Months are provided in the input config
    # if pd.isnull(SpclId):
    #     SpclId = 'auto'
    # if SpclId != 'auto':
    #     query = query + f"AND W.SPLCLASSIFICATIONID = '{SpclId}'"
    #     print(query)
    
    if pd.isnull(Max_Number_Of_Topics):
        Max_Number_Of_Topics=10
    
    if pd.isnull(Past_Months):
        Past_Months=6   
        
    #Create a VP categories dictionary
    vp_dict = {f"StreetLights": f"'{MunicipalityId:s}_MC-03_SC-02_SPC-03', '{MunicipalityId:s}_MC-03_SC-02_SPC-04', '{MunicipalityId:s}_MC-03_SC-02_SPC-05', '{MunicipalityId:s}_MC-03_SC-02_SPC-01', '{MunicipalityId:s}_MC-03_SC-02_SPC-02', '{MunicipalityId:s}_MC-03_SC-02_SPC-06', '{MunicipalityId:s}_MC-03_SC-01_SPC-02', '{MunicipalityId:s}_MC-03_SC-01_SPC-01', '{MunicipalityId:s}_MC-03_SC-03_SPC-06', '{MunicipalityId:s}_MC-03_SC-03_SPC-04', '{MunicipalityId:s}_MC-03_SC-03_SPC-08', '{MunicipalityId:s}_MC-03_SC-03_SPC-05', '{MunicipalityId:s}_MC-03_SC-03_SPC-12', '{MunicipalityId:s}_MC-03_SC-03_SPC-02', '{MunicipalityId:s}_MC-03_SC-03_SPC-03', '{MunicipalityId:s}_MC-03_SC-03_SPC-07', '{MunicipalityId:s}_MC-03_SC-03_SPC-09', '{MunicipalityId:s}_MC-03_SC-03_SPC-10', '{MunicipalityId:s}_MC-03_SC-03_SPC-11'", "GeneralCleaning": f"'{MunicipalityId:s}_MC-01_SC-01_SPC-04', '{MunicipalityId:s}_MC-07_SC-03_SPC-02', '{MunicipalityId:s}_MC-08_SC-02_SPC-570', '{MunicipalityId:s}_MC-09_SC-01_SPC-07', '{MunicipalityId:s}_MC-09_SC-01_SPC-03', '{MunicipalityId:s}_MC-09_SC-01_SPC-08', '{MunicipalityId:s}_MC-09_SC-01_SPC-09', '{MunicipalityId:s}_MC-09_SC-01_SPC-04', '{MunicipalityId:s}_MC-09_SC-01_SPC-06', '{MunicipalityId:s}_MC-09_SC-01_SPC-10', '{MunicipalityId:s}_MC-09_SC-01_SPC-01', '{MunicipalityId:s}_MC-09_SC-01_SPC-02', '{MunicipalityId:s}_MC-09_SC-02_SPC-05', '{MunicipalityId:s}_MC-09_SC-02_SPC-04', '{MunicipalityId:s}_MC-09_SC-02_SPC-06', '{MunicipalityId:s}_MC-09_SC-02_SPC-03'", "Sidewalks": f"'{MunicipalityId:s}_MC_04_SC-02_SPC-01', '{MunicipalityId:s}_MC-06_SC-01_SPC-02', '{MunicipalityId:s}_MC-06_SC-01_SPC-05', '{MunicipalityId:s}_MC-06_SC-03_SPC-02', '{MunicipalityId:s}_MC-06_SC-03_SPC-05', '{MunicipalityId:s}_MC-06_SC-03_SPC-01', '{MunicipalityId:s}_MC-06_SC-03_SPC-04'", "Potholes": f"'{MunicipalityId:s}_MC-06_SC-01_SPC-01','{MunicipalityId:s}_MC-06_SC-02_SPC-01','{MunicipalityId:s}_MC-06_SC-02_SPC-04','{MunicipalityId:s}_MC-06_SC-03_SPC-11','{MunicipalityId:s}_MC-06_SC-03_SPC-07','{MunicipalityId:s}_MC-06_SC-03_SPC-06','{MunicipalityId:s}_MC-07_SC-01_SPC-01'", "ExcavationBarriers": f"'{MunicipalityId:s}_MC-06_SC-02_SPC-03','{MunicipalityId:s}_MC-06_SC-03_SPC-09','{MunicipalityId:s}_MC-06_SC-03_SPC-468'"}
    #Define the SQL query for input data
   
    
    if pd.isnull(SpclId):
        SpclId = 'auto'
        query = (f""" SELECT W.PXCREATEDATETIME, W.LATITUDE, W.LONGITUDE, W.MUNICIPALITYID, W.SUBMUNICIPALITYID, W.SUB_SUBMUNICIPALITYID, W.DISTRICTNAME, W.STREETNAME, W.MAINCLASSIFICATIONID, W.SUBCLASSIFICATIONID, W.SPLCLASSIFICATIONID, W.ISSUEDESCRIPTION from PEGADATA.PCA_MOMRA_RYD_CS_WORK w WHERE W.PYID LIKE 'INC-%' AND W.SPLCLASSIFICATIONID IN ({vp_dict[VP_Label]:s}) AND W.MUNICIPALITYID = '{MunicipalityId:s}' AND ISSUEDESCRIPTION is not null AND LATITUDE is not null AND LONGITUDE is not null AND  W.PXCREATEDATETIME >= add_months(sysdate, -{Past_Months}) """)
    if SpclId != 'auto':
        
        query = (f""" SELECT W.PXCREATEDATETIME, W.LATITUDE, W.LONGITUDE, W.MUNICIPALITYID, W.SUBMUNICIPALITYID, W.SUB_SUBMUNICIPALITYID, W.DISTRICTNAME, W.STREETNAME, W.MAINCLASSIFICATIONID, W.SUBCLASSIFICATIONID, W.SPLCLASSIFICATIONID, W.ISSUEDESCRIPTION from PEGADATA.PCA_MOMRA_RYD_CS_WORK w WHERE W.PYID LIKE 'INC-%' AND W.SPLCLASSIFICATIONID = '{SpclId}' AND W.MUNICIPALITYID = '{MunicipalityId:s}' AND ISSUEDESCRIPTION is not null AND LATITUDE is not null AND LONGITUDE is not null AND  W.PXCREATEDATETIME >= add_months(sysdate, -{Past_Months}) """)
        
        # query = query + f"AND W.SPLCLASSIFICATIONID = '{SpclId}'"
    
    #Execute the function to collect input data
    data = collect_data(query)
    Number_Of_Cases = data.shape[0]
    description = 'strCaseDescription'
    #data.to_csv(f"TM_{VP_Label}_input_003.csv",encoding='utf-8-sig', index = False)
    # print(query)
    
    #Apply removeNonArabicSymbols() to remove any character besides arabic digits, and arabic_normalization() to normalize alef, alef maksura, and teh marbuta
    data[description] = (data[description].apply(removeNonArabicSymbols)).apply(arabic_normalization)

    #Output the word frequency of input data
    #print_word_freq(data)
    
    #Convert to list
    corpus_list  = data[description].values.tolist()

    # Remove double spaces
    corpus_list = [re.sub('\s+', ' ', sent).strip() for sent in corpus_list]

    # Convert sentences to words
    data_words = list(sent_to_words(corpus_list))

    # Remove Stopwords
    data_words_nostops = remove_stopwords(data_words, VP_Label)
    
    # wordcloud generation
    """
    from collections import Counter
    counts = Counter(x for sublist in data_words_nostops for x in sublist)
    from wordcloud import WordCloud
    import arabic_reshaper
    from bidi.algorithm import get_display

    # make a new dictionary for word cloud with modified keys (text) to show arabic word properly in the image
    # follow https://amueller.github.io/word_cloud/auto_examples/arabic.html#sphx-glr-download-auto-examples-arabic-py
    # for the to the desired result

    counts_wordCloud = dict()
    for k, w in counts.items():
        k_new = arabic_reshaper.reshape(k)
        k_new = get_display(k_new)
        counts_wordCloud[k_new] = w

    wordcloud = WordCloud(background_color=None, mode='RGBA', max_words=2500, contour_width=3, contour_color='steelblue',font_path='C:/MOMRA/my_pandas_extentions/NotoNaskhArabic-Regular.ttf', colormap='GnBu', width=1920, height=1080)    
    wordcloud.generate_from_frequencies(counts_wordCloud)
    wordcloud.to_file(f"{VP_Label}_{MunicipalityId}.png")
    """

    #Build Model

    # Create Dictionary
    id2word = corpora.Dictionary(data_words_nostops)
    
    try:
        id2word.filter_extremes(no_below=50, no_above=0.5, keep_n=100000)
        
        # Create Corpus
        texts = data_words_nostops

        # Term Document Frequency
        corpus = [id2word.doc2bow(text) for text in texts]

        # Identify number of topics
        
        # Can take a long time to run.
        model_list, coherence_values = compute_coherence_values(dictionary=id2word, corpus=corpus, texts=data_words_nostops, start=2, limit=Max_Number_Of_Topics, step=1)

        # Get k resulting in higest coherence value
        max_value = max(coherence_values)
        max_index = coherence_values.index(max_value)

        # Select the model and print the topics
        optimal_model = model_list[max_index]
        model_topics = optimal_model.show_topics(formatted=False)

        # Pretty print
        keywords = print_output(optimal_model)

        # Arranging result
        res = export_output(keywords, MunicipalityId, VP_Label, Past_Months, Max_Number_Of_Topics, Number_Of_Cases, SpclId)
        
        # Export Engine output to MOMRA Staging table for further processing
        res.columns = map(str.lower, res.columns)
        write_engines_output_to_database(res, table_name ="topic_model_hist_tbl", if_exists ='append')
        res.to_pickle(r'C:\MoMRA\1_RawData\TopicModelSnapshot.pkl')
            
    except Exception as err:
        logging.exception("Topic model cannot run LDA over empty text or collection")
    