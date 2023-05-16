#!bin/bash
minio server ~/minio --console-address :9090 &
cd ml_keyboards/ml_keyboards
python3 scrapyCrawler.py
pkill -f "minio"
rm "ml_keyboards/results.csv"
