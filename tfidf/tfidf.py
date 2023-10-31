import random
import pandas as pd
from pprint import pprint
import string
import os
import time
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer, TfidfVectorizer, ENGLISH_STOP_WORDS
from pandas.api.types import is_numeric_dtype

num_tables = 50
top_k = 10


# TODO: PREPROCESSING
# TODO: need to discuss: tfidf have a built-in preprocessing function, should we worry about it?


def preprocess_text(text):
    if not isinstance(text, str):
        # print(f"not a string: {text}")
        text = str(text)
    # Convert text to lowercase
    text = text.lower()

    # Remove punctuation
    text = ''.join([char for char in text if char not in string.punctuation])
    # replace_list = ['.', '-', '<', '>', '=']
    # for char in replace_list:
    #     text = text.replace(char, '_')

    # Tokenize and remove stopwords
    tokens = text.split()
    # tokens = [word for word in tokens if word not in ENGLISH_STOP_WORDS]

    return '_'.join(tokens)


def csv_shrink(table_name, column_name):
    # result: write a shrank csv file to shrank_csv/shrank_{table_name}_{column_name}.csv
    # return the shrank csv file name
    csv = pd.read_csv('tables/t{}.csv'.format(table_name))

    def get_terms(t_name, col_name):
        # get the top k terms from table_col_term-tfidf_val/{table_name}/{column_name}.csv
        # return a list of the top k terms
        term = pd.read_csv(f'table_col_term-tfidf_val/{t_name}/{col_name}.csv', sep='\t')
        term_col_name = 'Unnamed: 0'
        term = term.iloc[:top_k, :1]
        term = term[term_col_name].tolist()
        return term

    def get_shrank_df(csv_file, t_name, col_name):
        # if the two columns contain the same top k tf-idf terms, then we add that row to the shrank csv
        # else we do nothing
        csv_file[col_name] = csv_file[col_name].apply(preprocess_text).tolist()
        terms = get_terms(t_name, col_name)
        # print(f'terms: {terms}')
        # print(f'len(terms): {len(terms)}')
        # print(f'len(csv[column_name]): {len(csv[column_name])}')
        rows_to_move = csv_file[csv[col_name].isin(terms)]
        # print(f'rows_to_move: {rows_to_move}')
        # pprint(f'terms: {terms}')
        shrank_csv = rows_to_move.copy()
        '''
        # Concatenate new_df and additional_df
        new_df = pd.concat([new_df, additional_df], ignore_index=True)
        '''
        return shrank_csv

    # preprocess the two columns
    if is_numeric_dtype(csv[column_name]):
        # randomly select a top k rows from the two columns
        pass
        return
    shrank_df = get_shrank_df(csv, table_name, column_name)
    os.makedirs(f'shrank_csv/{table_name}/', exist_ok=True)
    shrank_df.to_csv(f'shrank_csv/{table_name}/shrank_on_{column_name}.csv', index=False)


def main():
    data_arr = []
    for table in range(num_tables):
        data_arr.append(pd.read_csv('tables/t{}.csv'.format(table)))

    # start the timer
    # start = time.perf_counter()

    # mkdir
    os.makedirs('table_col_term-tfidf_val/', exist_ok=True)
    os.makedirs('table_col-topk-tfidf_term/', exist_ok=True)

    for i in range(num_tables):  # or we can run this for the whole data_arr
        # print(f"i: {i}")
        # rand = random.randint(0, num_tables)
        # rand = 15
        df = data_arr[i]
        corpus: [str] = []
        col_counter = 0
        col_match = {}
        for j, col_name in enumerate(df.columns):
            if is_numeric_dtype(df.iloc[:, j]):
                continue
            # TODO: need to remember the column name to match them later since we are skipping the numeric columns
            curr_col_str = ' '.join(df.iloc[:, j].apply(preprocess_text).tolist())
            col_match[col_counter] = col_name
            col_counter += 1
            # print(curr_col_str)
            corpus.append(curr_col_str)
        # pprint(corpus)
        cv = CountVectorizer()
        tfidf_matrix = cv.fit_transform(corpus)
        # print(tfidf_matrix.shape)
        tfidf_transformer = TfidfTransformer(smooth_idf=True, use_idf=True)
        tfidf_transformer.fit(tfidf_matrix)
        # print idf values
        idf = pd.DataFrame(tfidf_transformer.idf_, index=cv.get_feature_names_out(), columns=["idf_weights"])
        # sort ascending
        idf.sort_values(by=['idf_weights'])
        # pprint(idf)
        # idf.to_csv(f"{i}.csv", sep='\t')
        # count matrix
        count_vector = cv.transform(corpus)
        # tf-idf scores
        tf_idf_vector = tfidf_transformer.transform(count_vector)
        feature_names = cv.get_feature_names_out()

        # get tfidf vector for first document
        # print(f"in the {i}th csv table")
        # print(f"idf: {idf}\n")
        for k, docs in enumerate(tf_idf_vector):
            # print the scores
            # print(f"docs: {docs}")
            tfidf = pd.DataFrame(docs.T.todense(), index=feature_names, columns=["tfidf_weights"])
            tfidf.sort_values(by=["tfidf_weights"], ascending=False, inplace=True)
            # print(f"tfidf: {tfidf}")
            # print(f"docs: {docs}")
            # print(f"in the {k}th column with name: {df.columns[k]}")
            # print(f"tfidf: {tfidf}\n")
            os.makedirs(f'table_col_term-tfidf_val/{i}/', exist_ok=True)
            os.makedirs(f'table_col-topk-tfidf_term/{i}/', exist_ok=True)
            # print(f"top {top_k} tfidf terms in the {k}th column with name: {col_match[k]}: {tfidf.iloc[:top_k, :1].index.tolist()}")
            tfidf.to_csv(f"table_col_term-tfidf_val/{i}/{col_match[k]}.csv", sep='\t')
        # print("\n\n\n")
        # for name in feature_names:
        #     print(f"feature_names: {name}")
        # end = time.perf_counter()
        '''
        ANOTHER WAY TO DO TF-IDF
        
        # settings that you use for count vectorizer will go here 
        tfidf_vectorizer=TfidfVectorizer(use_idf=True) 
        # just send in all your docs here 
        tfidf_vectorizer_vectors=tfidf_vectorizer.fit_transform(docs)
        for docs in tfidf_vectorizer_vectors:
            # place tf-idf values in a pandas data frame 
            tfidf = pd.DataFrame(docs.T.todense(), index=tfidf_vectorizer.get_feature_names_out(), columns=["tfidf"]) df.sort_values(by=["tfidf"],ascending=False).to_dict()
            print(tfidf)
        '''
    # print(f"finished in {end - start} seconds")


if __name__ == "__main__":
    main()
    csv_shrink('0', 'fiscal_year')
