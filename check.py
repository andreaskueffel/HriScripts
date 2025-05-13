import os
import csv
import math
fft_values = []
subfolder_path = "D:\\Source\\Python\\HriFftReader\\HriFftLog"
order=15
bandwidth=2
limit=15

frequStep=9.765625
def process_csv_line_by_line(file_path, skiprows):
    with open(file_path, mode='r') as file:
        for _ in range(skiprows):
            next(file)
        csv_reader = csv.DictReader(file)
        print('Processing ' + os.path.basename(file_path))
        for row in csv_reader:
            dmc=row['DMC']
            timestamp=row['TimeStamp']
            rps=math.fabs(float(row['Drehzahl'])/60)
            # Get the bounds for the frequencies based on speed and order
            lowerBound=math.floor(rps*(order-bandwidth/2)/frequStep) # 0
            upperBound=math.ceil(rps*(order+bandwidth/2)/frequStep) # 850
            # fft_row = [float(row[f'Wert{i}']) for i in range(1, 851)]
            fft_row = [float(row[f'Wert{i}']) for i in range(lowerBound+1, upperBound+1)]
            
            # Check if any value in fft_row exceeds the limit
            if any(value >= limit for value in fft_row):
                limitExceededObject = [os.path.basename(file_path), timestamp, dmc, max(fft_row)]
                fft_values.append(limitExceededObject)
                print('Limit exceeded: ' + str(limitExceededObject))


skiprows = 32 # Header rows to skip

files = os.listdir(subfolder_path)
for file_name in files:
    if file_name.endswith(".CSV"):
        file_path = os.path.join(subfolder_path, file_name)
        process_csv_line_by_line(file_path, skiprows)

for row in fft_values[:50]:
    print(row)