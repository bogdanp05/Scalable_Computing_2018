import csv

schema = ['user','song','count']

#
# txt_file = "/Users/Yukiii/Desktop/csv/train_triplets.txt"
# csv_file = "/Users/Yukiii/Documents/9_Scalable_computing/project/Project/src/main/resources/csv/train_triplets.csv"


# Create a csv with the first 200k rows

txt_file = "/home/bogdan/school_tmp/sc/train_triplets.txt"
csv_file = "./Project/src/main/resources/csv/mao200k.csv"
in_txt = csv.reader(open(txt_file, "r"), delimiter = '\t')
out_csv = csv.writer(open(csv_file, 'w'))
out_csv.writerow([name for name in schema])
for i in range(0,200000):
    out_csv.writerow(next(in_txt))


# Create full csv

# txt_file = "train_triplets.txt"
# csv_file = "train_triplets.csv"
# in_txt = csv.reader(open(txt_file, "r"), delimiter = '\t')
# out_csv = csv.writer(open(csv_file, 'w'))
# out_csv.writerow([name for name in schema])
# out_csv.writerows(in_txt)

# # Create streaming csv with first 20k rows (we will only use the song ids)
# txt_file = "/home/bogdan/school_tmp/sc/train_triplets.txt"
# csv_file = "./Project/src/main/resources/csv/streaming20k.csv"
# in_txt = csv.reader(open(txt_file, "r"), delimiter = '\t')
# out_csv = csv.writer(open(csv_file, 'w'))
# # we don't need the column names
# # out_csv.writerow([name for name in schema])
# for i in range(0,20000):
#     out_csv.writerow(next(in_txt))