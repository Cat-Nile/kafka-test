import csv
import json

input_file = "fire.csv"
output_file = "fire.txt"

with open(input_file, "r", encoding="euc-kr", newline="") as f, \
        open(output_file, "w", encoding="utf-8", newline="") as o:
    reader = csv.reader(f)
    col_names = next(reader)
    i=0
    for cols in reader:
        i=i+1
        doc = {col_name: col for col_name, col in zip(col_names, cols)}
        print(json.dumps(doc, ensure_ascii=False), file=o)
        if i >= 100000:
            break