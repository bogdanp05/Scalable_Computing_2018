import csv

schema = ['user','song','count']
# with open('mao.txt', 'r') as in_file:
#     stripped = (line.strip() for line in in_file)
#     lines = (line.split(",") for line in stripped if line)
#     with open('mao.csv', 'w') as out_file:
#         writer = csv.writer(out_file)
#         writer.writerow([name for name in schema])
#         writer.writerows(lines)

txt_file = "/Users/Yukiii/Desktop/csv/train_triplets.txt"
csv_file = "/Users/Yukiii/Documents/9_Scalable_computing/project/Project/src/main/resources/csv/train_triplets.csv"

in_txt = csv.reader(open(txt_file, "r"), delimiter = '\t')
out_csv = csv.writer(open(csv_file, 'w'))
out_csv.writerow([name for name in schema])
out_csv.writerows(in_txt)