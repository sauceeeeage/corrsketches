import pandas as pd
import pprint
import os
import glob
import argparse

sets = []
dic = dict()
delete = []

def screen(folder_path):
    print(f"Folder path: {folder_path}")
    g = glob.glob(folder_path + "/*.csv")
    counter = 0
    for file in g:
        print(f"Processing file: {file}")
        dic[counter] = file
        df = pd.read_csv(file, sep=",", header=0)
        # tmp_set = set(frozenset(row) for row in df.values)
        df_strings = df.astype(str).agg(''.join, axis=1)
        tmp_set = set(df_strings)
        sets.append(tmp_set)
        counter += 1
        # pprint.pprint(df)
    print(f"Number of sets: {len(sets)}\n")
    # pprint.pprint(sets[1])
    # pprint.pprint(sets)
    for i in range(len(sets)):
        for j in range(i+1, len(sets)):
            intersection = len(sets[i].intersection(sets[j]))
            union = len(sets[i].union(sets[j]))
            jaccard = intersection / union
            # print(f"s{i} and s{j} intersection: {len(sets[i].intersection(sets[j]))}")
            # print(f"s{i} and s{j} union: {len(sets[i].union(sets[j]))}")
            # print(f"s{i} size: {len(sets[i])}")
            # print(f"s{j} size: {len(sets[j])}")
            if intersection == len(sets[i]) or intersection == len(sets[j]):
                # print("here")
                if len(sets[i]) > len(sets[j]):
                    delete.append(dic[j])
                    print(f"Set {j}({dic[j]}) is a subset of Set {i}({dic[i]})")
                elif len(sets[i]) < len(sets[j]):
                    delete.append(dic[i])
                    print(f"Set {i}({dic[i]}) is a subset of Set {j}({dic[j]})")

                print(f"Set {i} length: {len(sets[i])}")
                print(f"Set {j} length: {len(sets[j])}")
                print(f"s{i} and s{j} intersection: {len(sets[i].intersection(sets[j]))}")
                print(f"s{i} and s{j} union: {len(sets[i].union(sets[j]))}")
                print("Set %d and Set %d similarity: %f" % (i, j, len(sets[i].intersection(sets[j])) / len(sets[i].union(sets[j]))))
                print("----------------------------------------------")
                
    pprint.pprint(f"Sets to delete: {delete}")
    for file in delete:
        print(f"Deleting file: {file}")
        os.remove(file)

if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("folder", help="data folder path", type=str, default="datas/test_data/test2")
    args = argparser.parse_args()
    
    if not os.path.exists(args.folder):
        print("Folder not found")
        exit(1)
    
    screen(folder_path=args.folder)