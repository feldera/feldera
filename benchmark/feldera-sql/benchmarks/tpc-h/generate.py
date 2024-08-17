import os
from itertools import islice
from plumbum.cmd import rpk
import pandas as pd

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))

print('Pushing data to Kafka topic...')
for input in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
    print(input)
    data_csv = os.path.join(SCRIPT_DIR, 'data-large/' + input + '.csv')

    batch_size = 3000
    data = pd.read_csv(data_csv, delimiter="|", header=None)
    count = len(data.index)
    (rpk['topic', 'create', input])()
    for i in range(int((count - 1) / batch_size) + 1):
        csv = data.iloc[(i * batch_size):((i + 1) * batch_size)].to_csv(index=False, header=None)
        (rpk['topic', 'produce', input, '-f', '%v'] << csv)()