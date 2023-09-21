from collections import Counter
import pandas as pd

def print_word_freq(data):
    #Count top 300 words
    n = 300
    p = Counter(" ".join(data).split()).most_common(300)
    word_freq = pd.DataFrame(p, columns=['Word', 'Frequency'])
    word_freq.to_csv('original_word_freq.csv',encoding='utf-8-sig')
    #print(word_freq)

# def print_output(model):
#     # Pretty print
#     keywords = []
#     single_topic = []
#     for idx, topic in model.print_topics(-1):
#         print('Topic:',idx)
#         #Split into 10 words
#         for i in range(0, len(topic.split('+'))):
#             word = topic.split('+')[i]
#             single_topic.append(word)
#             print(f'\nWord {i+1}: ',word)
#         keywords.append(single_topic)
#     return keywords

def print_output(model):
    # Pretty print
    keywords = []
    for idx, topic in model.print_topics(-1):
        words = topic.split('+')
        #print(f'Topic: {idx}')
        #Split into 10 words
        #for word_num, word in enumerate(words):
            #word include the word + probability
            #print(f'Word {word_num}: {word}')
        keywords.append(words)
    return keywords