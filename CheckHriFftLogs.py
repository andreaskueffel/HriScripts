import os
import csv
import math
import logging
import argparse

fft_values = []
subfolder_path = "D:\\Source\\Python\\HriFftReader\\HriFftLog"
frequStep = 9.765625

# Set up logging to file and console
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler('check.log')
file_handler.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def process_csv_line_by_line(file_path, skiprows, order, bandwidth, limit):
    with open(file_path, mode='r') as file:
        for _ in range(skiprows):
            next(file)
        csv_reader = csv.DictReader(file)
        logging.info('Processing ' + os.path.basename(file_path))
        for row in csv_reader:
            dmc = row['DMC']
            timestamp = row['TimeStamp']
            rps = math.fabs(float(row['Drehzahl']) / 60)
            # Get the bounds for the frequencies based on speed and order
            lowerBound = math.floor(rps * (order - bandwidth / 2) / frequStep)
            upperBound = math.ceil(rps * (order + bandwidth / 2) / frequStep)
            fft_row = [float(row[f'Wert{i}']) for i in range(lowerBound + 1, upperBound + 1)]
            
            # Check if any value in fft_row exceeds the limit
            if any(value >= limit for value in fft_row):
                limitExceededObject = [os.path.basename(file_path), timestamp, dmc, max(fft_row)]
                fft_values.append(limitExceededObject)
                logging.warning('Limit exceeded: ' + str(limitExceededObject))

# Command line argument parsing
parser = argparse.ArgumentParser(description='Process CSV files.')
parser.add_argument('--order', type=int, default=1, help='Order value')
parser.add_argument('--bandwidth', type=int, default=1, help='Bandwidth value')
parser.add_argument('--limit', type=int, default=100, help='Limit value')
args = parser.parse_args()

skiprows = 32  # Header rows to skip

files = os.listdir(subfolder_path)
for file_name in files:
    if file_name.endswith(".CSV"):
        file_path = os.path.join(subfolder_path, file_name)
        process_csv_line_by_line(file_path, skiprows, args.order, args.bandwidth, args.limit)
