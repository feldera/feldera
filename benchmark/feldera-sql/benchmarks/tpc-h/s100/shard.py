#! /usr/bin/python3
import csv
import sys

n_orders = 0
with open('orders-full.csv') as csvfile:
    for row in csvfile:
        n_orders += 1
print(f'n_orders={n_orders}')

row_number = 0
n_shards = 10
last_orders = []
with open('orders-full.csv') as csvfile:
    reader = csv.reader(csvfile, delimiter='|')
    for shard in range(n_shards):
        n_rows = n_orders // n_shards
        if shard < n_orders % n_shards:
            n_rows += 1
        output = csv.writer(open(f'orders-{shard + 1}.csv', 'w'), delimiter='|')
        for row in range(n_rows):
            row = next(reader)
            last_order = row[0]
            output.writerow(row)
        last_orders += [int(last_order)]
print(last_orders)
with open('lineitem-full.csv') as csvfile:
    shard = -1
    for row in csv.reader(csvfile, delimiter='|'):
        if shard < 0 or int(row[0]) > last_orders[shard]:
            shard += 1
            writer = csv.writer(open(f'lineitem-{shard + 1}.csv', 'w'), delimiter='|')
        writer.writerow(row)
