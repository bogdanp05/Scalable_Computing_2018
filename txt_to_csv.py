import csv

schema = ['user','song','count']
with open('train_triplets.txt', 'r') as in_file:
    stripped = (line.strip() for line in in_file)
    lines = (line.split(",") for line in stripped if line)
    with open('train_triplets.csv', 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerow([name for name in schema])
        writer.writerows(lines)
