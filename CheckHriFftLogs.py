import os
import json
import csv
import math
import logging
import argparse

fft_values = []
max_values_perdmc = {}
subfolder_path = "D:\\Backup\\Machine01\\hridata"
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


def process_csv_line_by_line(file_path, skiprows, order, bandwidth, limit, writer):
    absmax = 0
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
            if fft_row:
                absmax = max(absmax, max(fft_row))  # Update absmax with the maximum value in fft_row
            # Check if any value in fft_row exceeds the limit
            if any(value >= limit for value in fft_row):
                limitExceededObject = [os.path.basename(file_path), timestamp, dmc, max(fft_row)]
                fft_values.append(limitExceededObject)
                if dmc not in max_values_perdmc or limitExceededObject[3] > max_values_perdmc[dmc][3]:
                    max_values_perdmc[dmc] = limitExceededObject
                writer.writerow(limitExceededObject)

                logging.warning('Limit exceeded: ' + str(limitExceededObject))
        logging.info('Processed ' + os.path.basename(file_path) + ' with max value: ' + str(absmax))
                



def extract_data_from_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        monitoring_frequencies = data.get("MonitoringFrequencies", [])
        
        extracted_data = []
        for item in monitoring_frequencies:
            order = item.get("Order")
            bandwidth = item.get("BandwidthOrd")
            error_level = item.get("ErrorLevel")
            reaction = item.get("Reaction")
            if error_level < 5000 and reaction > 0:
                extracted_data.append({
                    "FileName": os.path.basename(file_path),
                    "Order": order,
                    "BandwidthOrd": bandwidth,
                    "ErrorLevel": error_level,
                    "Reaction": reaction
        })  
        return extracted_data

def extract_data_from_folder(folder_path):
    all_extracted_data = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".json"):
            file_path = os.path.join(folder_path, file_name)
            extracted_data = extract_data_from_json(file_path)
            all_extracted_data.extend(extracted_data)
    return all_extracted_data








# Command line argument parsing
parser = argparse.ArgumentParser(description='Process CSV files.')
parser.add_argument('--order', type=int, default=16, help='Order value') #16
parser.add_argument('--bandwidth', type=int, default=20, help='Bandwidth value') #20
parser.add_argument('--limit', type=int, default=100, help='Limit value') #300
args = parser.parse_args()

# Extract data from all JSON files in the folder
extracted_data = extract_data_from_folder(subfolder_path+ "\\config\\partprograms")

with open('extracted_data.csv', mode='w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['FileName', 'Order', 'BandwidthOrd', 'ErrorLevel', 'Reaction']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    if csvfile.tell() == 0:
        writer.writeheader()
    for data in extracted_data:
    # Write extracted data to a CSV file
        print(data)
        writer.writerow(data)


skiprows = 32  # Header rows to skip

with open('OrderExceededLines.csv', mode='w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['FileName', 'Timestamp', 'DMC', 'MaxValue']) # Header
    files = os.listdir(subfolder_path+ "\\production\\HriFFTLog")
    for file_name in files:
        if file_name.endswith(".CSV") and file_name.startswith("50_"):
            file_path = os.path.join(subfolder_path+ "\\production\\HriFFTLog", file_name)
            process_csv_line_by_line(file_path, skiprows, args.order, args.bandwidth, args.limit, writer)

with open('OrderExceededDMCs.csv', mode='w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['FileName', 'Timestamp', 'DMC', 'MaxValue']) # Header
    for dmc, values in max_values_perdmc.items():
        writer.writerow(values)
        
