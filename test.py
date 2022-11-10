


with open('fire.csv', 'r', encoding='euc-kr') as obj:
    csv_reader = reader(obj)
    header = next(csv_reader)
    print(header)