import gensim
from gensim.models import CoherenceModel
import re
import gensim.corpora as corporas


def compute_coherence_values(dictionary, corpus, texts, limit, start=2, step=3):
    """
    Compute c_v coherence for various number of topics

    Parameters:
    ----------
    dictionary : Gensim dictionary
    corpus : Gensim corpus
    texts : List of input texts
    limit : Max num of topics

    Returns:
    -------
    model_list : List of LDA topic models
    coherence_values : Coherence values corresponding to the LDA model with respective number of topics
    """
    #Maximum number of topics is 10 topics
    if limit > 10:
        limit = 10
    #Minimum number of topics is 10 topics
    if limit < 2:
        limit = 2
        
    coherence_values = []
    model_list = []
    for num_topics in range(start, limit, step):
        #Build LDA model
        model = gensim.models.ldamodel.LdaModel(corpus=corpus,id2word=dictionary,num_topics=num_topics,random_state=100, update_every=1, chunksize=100,passes=10,alpha='auto', per_word_topics=True)
        model_list.append(model)
        #Build Coherence model
        coherencemodel = CoherenceModel(model=model, texts=texts, dictionary=dictionary, coherence='c_v')
        #Save coherence score
        coherence_values.append(coherencemodel.get_coherence())

    return model_list, coherence_values

