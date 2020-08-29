# Import required libraries
import nltk

# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('stopwords')

from nltk import word_tokenize, PorterStemmer
from nltk import tokenize
from nltk.tokenize import RegexpTokenizer
# from nltk.corpus import stopwords
tokenizer = RegexpTokenizer(r'\w+')
import re
import math
import scipy.stats as stats

stop_words = []
with open("stopwords.txt", "r", encoding="utf-8") as f:
    for line in f:
        stop_words.append(str(line.strip()))
        
new_stp_wrds = ['00','abbã³l', 'acaba', 'acerca', 'aderton', 'ahimã', 'ain', 'akã', 'alapjã', 'alors', 'alã', 'alã³l', 'alã³la', 'alã³lad', 'alã³lam', 'alã³latok', 'alã³luk', 'alã³lunk', 'amã', 'annã', 'appendix', 'arrã³l', 'attã³l', 'azokbã³l', 'azokkã', 'azoknã', 'azokrã³l', 'azoktã³l', 'azokã', 'aztã', 'azzã', 'azã', 'ba', 'bahasa', 'bb', 'bban', 'bbi', 'bbszã', 'belã', 'belã¼l', 'belå', 'bennã¼k', 'bennã¼nk', 'bã', 'bãºcsãº', 'cioã', 'cittã', 'ciã²', 'conjunctions', 'cosã', 'couldn', 'csupã', 'daren', 'didn', 'dik', 'diket', 'doesn', 'don', 'dovrã', 'ebbå', 'effects', 'egyedã¼l', 'egyelå', 'egymã', 'egyã', 'egyã¼tt', 'egã', 'ek', 'ellenã', 'elså', 'elã', 'elå', 'ennã', 'enyã', 'ernst', 'errå', 'ettå', 'ezekbå', 'ezekkã', 'ezeknã', 'ezekrå', 'ezektå', 'ezekã', 'ezentãºl', 'ezutã', 'ezzã', 'ezã', 'felã', 'forsûke', 'fã', 'fûr', 'fûrst', 'ged', 'gen', 'gis', 'giã', 'gjûre', 'gre', 'gtã', 'gy', 'gyet', 'gã', 'gã³ta', 'gã¼l', 'gã¼le', 'gã¼led', 'gã¼lem', 'gã¼letek', 'gã¼lã¼k', 'gã¼lã¼nk', 'hadn', 'hallã³', 'hasn', 'haven', 'herse', 'himse', 'hiã', 'hozzã', 'hurrã', 'hã', 'hãºsz', 'idã', 'ig', 'igazã', 'immã', 'indonesia', 'inkã', 'insermi', 'ismã', 'isn', 'juk', 'jã', 'jã³', 'jã³l', 'jã³lesik', 'jã³val', 'jã¼k', 'kbe', 'kben', 'kbå', 'ket', 'kettå', 'kevã', 'khã', 'kibå', 'kikbå', 'kikkã', 'kiknã', 'kikrå', 'kiktå', 'kikã', 'kinã', 'kirå', 'kitå', 'kivã', 'kiã', 'kkel', 'knek', 'knã', 'korã', 'kre', 'krå', 'ktå', 'kã', 'kã¼lã', 'lad', 'lam', 'latok', 'ldã', 'led', 'leg', 'legalã', 'lehetå', 'lem', 'lennã', 'leszã¼nk', 'letek', 'lettã¼nk', 'ljen', 'lkã¼l', 'll', 'lnak', 'ltal', 'ltalã', 'luk', 'lunk', 'lã', 'lã¼k', 'lã¼nk', 'magã', 'manapsã', 'mayn', 'megcsinã', 'mellettã¼k', 'mellettã¼nk', 'mellã', 'mellå', 'mibå', 'mightn', 'mikbå', 'mikkã', 'miknã', 'mikrå', 'miktå', 'mikã', 'mindenã¼tt', 'minã', 'mirå', 'mitå', 'mivã', 'miã', 'modal', 'mostanã', 'mustn', 'myse', 'mã', 'mãºltkor', 'mãºlva', 'må', 'måte', 'nak', 'nbe', 'nben', 'nbã', 'nbå', 'needn', 'nek', 'nekã¼nk', 'nemrã', 'nhetå', 'nhã', 'nk', 'nnek', 'nnel', 'nnã', 'nre', 'nrå', 'nt', 'ntå', 'nyleg', 'nyszor', 'nã', 'nå', 'når', 'også', 'ordnung', 'oughtn', 'particles', 'pen', 'perchã', 'perciã²', 'perã²', 'pest', 'piã¹', 'puã²', 'pã', 'quelqu', 'qué', 'ra', 'rcsak', 'rem', 'retrieval', 'rlek', 'rmat', 'rmilyen', 'rom', 'rt', 'rte', 'rted', 'rtem', 'rtetek', 'rtã¼k', 'rtã¼nk', 'rã', 'rã³la', 'rã³lad', 'rã³lam', 'rã³latok', 'rã³luk', 'rã³lunk', 'rã¼l', 'sarã', 'schluss', 'semmisã', 'shan', 'shouldn', 'sik', 'sikat', 'snap', 'sodik', 'sodszor', 'sokat', 'sokã', 'sorban', 'sorã', 'sra', 'st', 'stb', 'stemming', 'study', 'sz', 'szen', 'szerintã¼k', 'szerintã¼nk', 'szã', 'sã', 'talã', 'ted', 'tegnapelå', 'tehã', 'tek', 'tessã', 'tha', 'tizenhã', 'tizenkettå', 'tizenkã', 'tizennã', 'tizenã', 'tok', 'tovã', 'tszer', 'tt', 'tte', 'tted', 'ttem', 'ttetek', 'ttã¼k', 'ttã¼nk', 'tulsã³', 'tven', 'tã', 'tãºl', 'tå', 'ul', 'utoljã', 'utolsã³', 'utã', 'vben', 'vek', 'velã¼k', 'velã¼nk', 'verbs', 'ves', 'vesen', 'veskedjã', 'viszlã', 'viszontlã', 'volnã', 'vvel', 'vã', 'vå', 'vöre', 'vört', 'wahr', 'wasn', 'weren', 'won', 'wouldn', 'zadik', 'zat', 'zben', 'zel', 'zepesen', 'zepã', 'zã', 'zã¼l', 'zå', 'ã³ta', 'ãºgy', 'ãºgyis', 'ãºgynevezett', 'ãºjra', 'ãºr', 'ð¾da', 'γα', 'البت', 'بالای', 'برابر', 'برای', 'بیرون', 'تول', 'توی', 'تی', 'جلوی', 'حدود', 'خارج', 'دنبال', 'روی', 'زیر', 'سری', 'سمت', 'سوی', 'طبق', 'عقب', 'عل', 'عنوان', 'قصد', 'لطفا', 'مد', 'نزد', 'نزدیک', 'وسط', 'پاعین', 'کنار', 'अपन', 'अभ', 'इत', 'इनक', 'इसक', 'इसम', 'उनक', 'उसक', 'एव', 'ऐस', 'करत', 'करन', 'कह', 'कहत', 'गय', 'जह', 'तन', 'तर', 'दब', 'दर', 'धर', 'नस', 'नह', 'पहल', 'बन', 'बह', 'यत', 'यद', 'रख', 'रह', 'लक', 'वर', 'वग़', 'सकत', 'सबस', 'सभ', 'सर', 'ἀλλ']       
final_stp_wrds = stop_words + new_stp_wrds
stopWords = final_stp_wrds


""" pos_tag_narratives accept sentences from blogpost and with the help of grammar rules, extract VerbPhrases, NounPhrases, and Triplets from each sentence """
def pos_tag_narratives(textSentString):
    token = word_tokenize(textSentString)
    tags = nltk.pos_tag(token)
    grammar = r"""
      NP: {<DT|JJ|NN.*>+}
          {<IN>?<NN.*>}
      VP: {<TO>?<VB.*>+<IN>?<RB.*>?}
      CLAUSE: {<CD>?<NP><VP>+<NP>?<TO>?<NP>?<IN>?<NP>?<VP>?<NP>?<TO>?<NP>+}
    """
    a = nltk.RegexpParser(grammar)
    result = a.parse(tags)
    tfidf_string = ''
    for a in result:
        if type(a) is nltk.Tree:
            str1= ''
            if a.label() == 'CLAUSE':
            # This climbs into your NVN tree
                for b in a:
                    if(isinstance(b, tuple)):
                        #print(b[0])
                        str1 += str(b[0])+ ' '
                    else:
                        for elem in b:
                            str1 += str(elem[0])+ ' '
                        #print(b.leaves()) # This outputs your "NP"
                str1 = str1.strip() + str('.') + str(' ')
                tfidf_string += str1
    return tfidf_string

""" _create_frequency_table method accepts a sentence and creates frequency for each word """
def _create_frequency_table(text_string):
    """
    we create a dictionary for the word frequency table.
    For this, we should only use the words that are not part of the stopWords array.
    Removing stop words and making frequency table
    Stemmer - an algorithm to bring words to its root word.
    :rtype: dict
    """
    # stopWords = set(stopwords.words("english"))
    

    words = word_tokenize(text_string)
    ps = PorterStemmer()

    freqTable = dict()
    for word in words:
        word = ps.stem(word)
        if word in stopWords:
            continue
        if word in freqTable:
            freqTable[word] += 1
        else:
            freqTable[word] = 1

    return freqTable

""" _create_frequency_matrix creates a matrix of sentences for a given blogpost """
def _create_frequency_matrix(sentences):
    frequency_matrix = {}
    # stopWords = set(stopwords.words("english"))
    

    ps = PorterStemmer()
    for sent in sentences:
        freq_table = {}
        words = word_tokenize(sent)
        for word in words:
            word = word.lower()
            word = ps.stem(word)
            if word in stopWords:
                continue
            if word in freq_table:
                freq_table[word] += 1
            else:
                freq_table[word] = 1
        #frequency_matrix[sent[:15]] = freq_table
        frequency_matrix[sent] = freq_table
    return frequency_matrix

""" Using freq_matrix creates a TermFreq Matrix """
def _create_tf_matrix(freq_matrix):
    tf_matrix = {}
    for sent, f_table in freq_matrix.items():
        tf_table = {}
        count_words_in_sentence = len(f_table)
        for word, count in f_table.items():
            tf_table[word] = count / count_words_in_sentence
        tf_matrix[sent] = tf_table
    return tf_matrix

""" Using freq_matrix created words for document """
def _create_documents_per_words(freq_matrix):
    word_per_doc_table = {}
    for sent, f_table in freq_matrix.items():
        for word, count in f_table.items():
            if word in word_per_doc_table:
                word_per_doc_table[word] += 1
            else:
                word_per_doc_table[word] = 1
    return word_per_doc_table

""" this method creates Inverse Document Freq matrix """
def _create_idf_matrix(freq_matrix, count_doc_per_words, total_documents):
    idf_matrix = {}
    for sent, f_table in freq_matrix.items():
        idf_table = {}
        for word in f_table.keys():
            #Considering DF only
            idf_table[word] = math.log10(float(count_doc_per_words[word])/total_documents)
        idf_matrix[sent] = idf_table
    return idf_matrix

def _create_tf_idf_matrix(tf_matrix, idf_matrix):
    tf_idf_matrix = {}
    for (sent1, f_table1), (sent2, f_table2) in zip(tf_matrix.items(), idf_matrix.items()):
        tf_idf_table = {}
        for (word1, value1), (word2, value2) in zip(f_table1.items(),f_table2.items()):  # here, keys are the same in both the table
            tf_idf_table[word1] = float(value1 * value2)
        tf_idf_matrix[sent1] = tf_idf_table
    return tf_idf_matrix

def _score_sentences(tf_idf_matrix):
    sentenceValue = {}
    for sent, f_table in tf_idf_matrix.items():
        total_score_per_sentence = 0
        count_words_in_sentence = len(f_table)
        for word, score in f_table.items():
            total_score_per_sentence += score
        sentenceValue[sent] = total_score_per_sentence / count_words_in_sentence
    return sentenceValue

def _tuning_tuningParam(tuningParam):
    tuningParamValue = tuningParam   
    return tuningParamValue

def _find_average_score(sentenceValue):
    """
    Find the average score from the sentence value dictionary
    :rtype: int
    """
    sumValues = 0
    for entry in sentenceValue:    
        sumValues += sentenceValue[entry]
    
    try:
        average = (sumValues / len(sentenceValue))
    except:
        average = 0
    return average

def _find_z_score(sentenceValue):
    """
    Find the average score from the sentence value dictionary
    :rtype: int
    """
    keys, vals = zip(*sentenceValue.items())
    z = stats.zscore(vals)
    #print(np.sort(z)[::-1])
    z_value = dict(zip(keys,z))
    return z_value

def _generate_narratives(sentences, sentenceValue, threshold):
    sentence_count = 0
    summary = ''
    dict_sent = {}
    for sentence in sentences:
        #if sentence[:15] in sentenceValue and sentenceValue[sentence[:15]] >= (threshold):
        if sentence in sentenceValue:
            dict_sent[sentence] = sentenceValue[sentence]
    sentences_sorted_values = sorted(dict_sent, key=dict_sent.get, reverse=True)
    count =0
    for r in sentences_sorted_values:
        #This is tuning parameter to display only top sentences.
        #print(r)
        if(count ==100):
            break
        count = count + 1
        #print(r, dict_sent[r])
        summary += str(r) + " "
        sentence_count += 1
    return summary

""" This method will comprehensively do all the required things for us. First accept sentences and
    does Frequency matrix operation, TF, IDF, TF-IDF, scoring sentences """
def run_comprehensive(text):
    # 1 Sentence Tokenize
    sentences = tokenize.sent_tokenize(text)
    #print(sentences)
    total_documents = len(sentences)
    #print(total_documents)
    #print(sentences)

    # 2 Create the Frequency matrix of the words in each sentence.
    freq_matrix = _create_frequency_matrix(sentences)
    #print(freq_matrix)

    # 3 Calculate TermFrequency and generate a matrix
    tf_matrix = _create_tf_matrix(freq_matrix)
    #print(tf_matrix)

    # 4 creating table for documents per words
    count_doc_per_words = _create_documents_per_words(freq_matrix)
    #print(count_doc_per_words)

    # 5 Calculate IDF and generate a matrix
    idf_matrix = _create_idf_matrix(freq_matrix, count_doc_per_words, total_documents)
    #print(idf_matrix)

    # 6 Calculate TF-IDF and generate a matrix
    tf_idf_matrix = _create_tf_idf_matrix(tf_matrix, idf_matrix)
    #print(tf_idf_matrix)

    # 7 Important Algorithm: score the sentences
    sentence_scores = _score_sentences(tf_idf_matrix)
    #print(sentence_scores)

    # 8 Find the threshold
    threshold = _find_average_score(sentence_scores)
    
    #z_value = _find_z_score(sentence_scores)
    #print(z_value)
     
    # 9 Important Algorithm: Generate the narratives
    narratives = _generate_narratives(sentences, sentence_scores, threshold)
    #narratives_z = _generate_narratives(sentences, sentence_scores, z_value)
    return narratives

""" Takes the scored sentences and filters based on Entities in the sentence and limits 5 per blog post. Also does clean up on sentences """
# def single_entity_narratives(sentences_scoredList, objectEntitiesList):
#     ListSingleEntity_Filtered =[]
#     ListNoEntities_Filtered = []
#     update_Count = 0
#     #print(len(sentences_scoredList))
#     for elem in sentences_scoredList:
#         # flagCheck = False
#         # flagCount =0
#         elem = elem[:-1]
#         count = 0
#         elem_split = elem.split(' ')
#         for elems in elem_split:
#             for listitems in objectEntitiesList:
#                 if (listitems.lower() == elems.lower()):
#                     count = count +1
#                     if (count == 1):
#                         flagCheck = True
#                         elem = elem.capitalize()
#                         elem = elem + str('.')
#                         #print(elem)
#                         elem = elem.replace(" a. ", " ")
#                         elem = elem.replace(" r ", "r ") 
#                         elem = elem.replace(" t ", "t ")
#                         elem = elem.replace(" ] ", " ")
#                         if("https" in elem or "t.co" in elem):
#                             continue         
#                         ListSingleEntity_Filtered.append(elem)
#                         break
#     for elem in sentences_scoredList:
#         ListSingleEntity_Filtered_Lower = [item.lower() for item in ListSingleEntity_Filtered]
#         if elem.lower() not in ListSingleEntity_Filtered_Lower:
#             ListNoEntities_Filtered.append(elem) 
#     if(len(ListSingleEntity_Filtered)>4):
#         for x in range(5):
#             # print(str(ListSingleEntity_Filtered[x]))
#             outputfile.write(str(ListSingleEntity_Filtered[x]))
#             outputfile.write('\n')
#             outputfile.flush()         
#     else:
#         update_Count = 5 - len(ListSingleEntity_Filtered)
#         for eachNarrative in ListSingleEntity_Filtered:
#             outputfile.write(str(eachNarrative))
#             outputfile.write('\n')
#             outputfile.flush()
#         for y in range(update_Count):
#             try:
#                 outputfile.write(str(ListNoEntities_Filtered[y]))
#                 outputfile.write('\n')
#                 outputfile.flush()
#             except:
#                 pass         
# def get_stop_words():
#     stop_words = []
#     with open("stopwords.txt", "r", encoding="utf-8") as f:
#         for line in f:
#             stop_words.append(str(line.strip()))
            
#     new_stp_wrds = ['00','abbã³l', 'acaba', 'acerca', 'aderton', 'ahimã', 'ain', 'akã', 'alapjã', 'alors', 'alã', 'alã³l', 'alã³la', 'alã³lad', 'alã³lam', 'alã³latok', 'alã³luk', 'alã³lunk', 'amã', 'annã', 'appendix', 'arrã³l', 'attã³l', 'azokbã³l', 'azokkã', 'azoknã', 'azokrã³l', 'azoktã³l', 'azokã', 'aztã', 'azzã', 'azã', 'ba', 'bahasa', 'bb', 'bban', 'bbi', 'bbszã', 'belã', 'belã¼l', 'belå', 'bennã¼k', 'bennã¼nk', 'bã', 'bãºcsãº', 'cioã', 'cittã', 'ciã²', 'conjunctions', 'cosã', 'couldn', 'csupã', 'daren', 'didn', 'dik', 'diket', 'doesn', 'don', 'dovrã', 'ebbå', 'effects', 'egyedã¼l', 'egyelå', 'egymã', 'egyã', 'egyã¼tt', 'egã', 'ek', 'ellenã', 'elså', 'elã', 'elå', 'ennã', 'enyã', 'ernst', 'errå', 'ettå', 'ezekbå', 'ezekkã', 'ezeknã', 'ezekrå', 'ezektå', 'ezekã', 'ezentãºl', 'ezutã', 'ezzã', 'ezã', 'felã', 'forsûke', 'fã', 'fûr', 'fûrst', 'ged', 'gen', 'gis', 'giã', 'gjûre', 'gre', 'gtã', 'gy', 'gyet', 'gã', 'gã³ta', 'gã¼l', 'gã¼le', 'gã¼led', 'gã¼lem', 'gã¼letek', 'gã¼lã¼k', 'gã¼lã¼nk', 'hadn', 'hallã³', 'hasn', 'haven', 'herse', 'himse', 'hiã', 'hozzã', 'hurrã', 'hã', 'hãºsz', 'idã', 'ig', 'igazã', 'immã', 'indonesia', 'inkã', 'insermi', 'ismã', 'isn', 'juk', 'jã', 'jã³', 'jã³l', 'jã³lesik', 'jã³val', 'jã¼k', 'kbe', 'kben', 'kbå', 'ket', 'kettå', 'kevã', 'khã', 'kibå', 'kikbå', 'kikkã', 'kiknã', 'kikrå', 'kiktå', 'kikã', 'kinã', 'kirå', 'kitå', 'kivã', 'kiã', 'kkel', 'knek', 'knã', 'korã', 'kre', 'krå', 'ktå', 'kã', 'kã¼lã', 'lad', 'lam', 'latok', 'ldã', 'led', 'leg', 'legalã', 'lehetå', 'lem', 'lennã', 'leszã¼nk', 'letek', 'lettã¼nk', 'ljen', 'lkã¼l', 'll', 'lnak', 'ltal', 'ltalã', 'luk', 'lunk', 'lã', 'lã¼k', 'lã¼nk', 'magã', 'manapsã', 'mayn', 'megcsinã', 'mellettã¼k', 'mellettã¼nk', 'mellã', 'mellå', 'mibå', 'mightn', 'mikbå', 'mikkã', 'miknã', 'mikrå', 'miktå', 'mikã', 'mindenã¼tt', 'minã', 'mirå', 'mitå', 'mivã', 'miã', 'modal', 'mostanã', 'mustn', 'myse', 'mã', 'mãºltkor', 'mãºlva', 'må', 'måte', 'nak', 'nbe', 'nben', 'nbã', 'nbå', 'needn', 'nek', 'nekã¼nk', 'nemrã', 'nhetå', 'nhã', 'nk', 'nnek', 'nnel', 'nnã', 'nre', 'nrå', 'nt', 'ntå', 'nyleg', 'nyszor', 'nã', 'nå', 'når', 'også', 'ordnung', 'oughtn', 'particles', 'pen', 'perchã', 'perciã²', 'perã²', 'pest', 'piã¹', 'puã²', 'pã', 'quelqu', 'qué', 'ra', 'rcsak', 'rem', 'retrieval', 'rlek', 'rmat', 'rmilyen', 'rom', 'rt', 'rte', 'rted', 'rtem', 'rtetek', 'rtã¼k', 'rtã¼nk', 'rã', 'rã³la', 'rã³lad', 'rã³lam', 'rã³latok', 'rã³luk', 'rã³lunk', 'rã¼l', 'sarã', 'schluss', 'semmisã', 'shan', 'shouldn', 'sik', 'sikat', 'snap', 'sodik', 'sodszor', 'sokat', 'sokã', 'sorban', 'sorã', 'sra', 'st', 'stb', 'stemming', 'study', 'sz', 'szen', 'szerintã¼k', 'szerintã¼nk', 'szã', 'sã', 'talã', 'ted', 'tegnapelå', 'tehã', 'tek', 'tessã', 'tha', 'tizenhã', 'tizenkettå', 'tizenkã', 'tizennã', 'tizenã', 'tok', 'tovã', 'tszer', 'tt', 'tte', 'tted', 'ttem', 'ttetek', 'ttã¼k', 'ttã¼nk', 'tulsã³', 'tven', 'tã', 'tãºl', 'tå', 'ul', 'utoljã', 'utolsã³', 'utã', 'vben', 'vek', 'velã¼k', 'velã¼nk', 'verbs', 'ves', 'vesen', 'veskedjã', 'viszlã', 'viszontlã', 'volnã', 'vvel', 'vã', 'vå', 'vöre', 'vört', 'wahr', 'wasn', 'weren', 'won', 'wouldn', 'zadik', 'zat', 'zben', 'zel', 'zepesen', 'zepã', 'zã', 'zã¼l', 'zå', 'ã³ta', 'ãºgy', 'ãºgyis', 'ãºgynevezett', 'ãºjra', 'ãºr', 'ð¾da', 'γα', 'البت', 'بالای', 'برابر', 'برای', 'بیرون', 'تول', 'توی', 'تی', 'جلوی', 'حدود', 'خارج', 'دنبال', 'روی', 'زیر', 'سری', 'سمت', 'سوی', 'طبق', 'عقب', 'عل', 'عنوان', 'قصد', 'لطفا', 'مد', 'نزد', 'نزدیک', 'وسط', 'پاعین', 'کنار', 'अपन', 'अभ', 'इत', 'इनक', 'इसक', 'इसम', 'उनक', 'उसक', 'एव', 'ऐस', 'करत', 'करन', 'कह', 'कहत', 'गय', 'जह', 'तन', 'तर', 'दब', 'दर', 'धर', 'नस', 'नह', 'पहल', 'बन', 'बह', 'यत', 'यद', 'रख', 'रह', 'लक', 'वर', 'वग़', 'सकत', 'सबस', 'सभ', 'सर', 'ἀλλ']       
#     final_stp_wrds = stop_words + new_stp_wrds
#     stopWords = final_stp_wrds

#     return stopWords

"""Getting top entities, narratives and posts"""
def entity_narratives(sentences_scoredList, blogpost_id, objectEntitiesList, entity_count = []):
    entity_narratives_dict = {}
    
    for narr in sentences_scoredList:
        for entity in objectEntitiesList:
            temp = " " + entity.lower() + " "
            if temp in narr.lower() and len(entity) > 1:
                if entity not in entity_narratives_dict:
                    entity_narratives_dict[entity] = [{"narrative":narr, "blogpost_id": blogpost_id}]
                    entity_count.append({'term':entity, 'occurrence': 1})
                else:
                    entity_narratives_dict[entity].append({"narrative":narr, "blogpost_id": blogpost_id})
                    res = [(i, x['occurrence']) for i,x in enumerate(entity_count) if x['term'] == entity] 
                    idx = res[0][0]
                    new_count = res[0][1] + 1
                    entity_count[idx] = {'term':entity,'occurrence':new_count}


    return entity_narratives_dict