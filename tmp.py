srcFilePath = '/home/kscott/apps/DataLakes/raw/NAD_r11.txt'
destColName = 'nationaladdress'

f = open(file=srcFilePath, mode='r', newline='\n')

line = f.readline()
header = line.split(',')


json_array = []
for line in f.readlines() :
    data = line.split(',') 

    json_data = {}
    if len(header) == len(data) :        
        json_data = {k:v for k,v in zip(header,data)}

    json_array.append({'has_json': len(header)==len(data), 'header': header, 'raw_data': line, 'json_data': json_data})
    print(f'{json_data}\n\r')

print(json_array)
print(f'{f.closed}')




