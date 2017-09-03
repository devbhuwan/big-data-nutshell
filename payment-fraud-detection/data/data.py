import csv
import random
import uuid
import datetime

with open("payment.csv", 'wb') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    for num in range(1, 10000):
        wr.writerow([str(uuid.uuid4()), "ProgressSoft", str(random.randint(1000000000000,9999999999999999)),"DUBAI", str(random.randint(10000,9999999)), str(datetime.datetime.now())])
