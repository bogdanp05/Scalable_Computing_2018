import csv

schema = ['user','song','count']

#
# txt_file = "/Users/Yukiii/Desktop/csv/train_triplets.txt"
# csv_file = "/Users/Yukiii/Documents/9_Scalable_computing/project/Project/src/main/resources/csv/train_triplets.csv"


# Create a csv with the first 2k rows
txt_file = "train_triplets.txt"
csv_file = "mao2.csv"
in_txt = csv.reader(open(txt_file, "r"), delimiter = '\t')
out_csv = csv.writer(open(csv_file, 'w'))
out_csv.writerow([name for name in schema])
for i in range(0,2000):
    out_csv.writerow(next(in_txt))


# Create full csv

# txt_file = "train_triplets.txt"
# csv_file = "train_triplets.csv"
# in_txt = csv.reader(open(txt_file, "r"), delimiter = '\t')
# out_csv = csv.writer(open(csv_file, 'w'))
# out_csv.writerow([name for name in schema])
# out_csv.writerows(in_txt)