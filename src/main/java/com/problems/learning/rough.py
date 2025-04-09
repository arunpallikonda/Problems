import gzip
import random
import string
from datetime import datetime, timedelta

def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def generate_random_dict():
    return {
        "id": random.randint(1000, 9999),
        "name": random_string(10),
        "timestamp": random_date(datetime(2020, 1, 1), datetime(2025, 1, 1)).isoformat(),
        "active": random.choice([True, False]),
        "score": round(random.uniform(0, 100), 2)
    }

def generate_gz_file(filename, num_records):
    with gzip.open(filename, 'wt', encoding='utf-8') as f:
        for _ in range(num_records):
            record = generate_random_dict()
            f.write(str(record) + '\n')

if __name__ == "__main__":
    generate_gz_file("random_dicts.gz", num_records=1000000)  # Change this number to control file size
