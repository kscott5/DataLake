# National Address Database CSV import
The [NAD](https://www.transportation.gov/gis/national-address-database) is a list of address within the United States from states that
thought sharing their local data was safe. The CSV data file is 17 gigbytes
in size.

My plan was simple import with MongoImport. However, [MongoImport](https://www.mongodb.com/docs/database-tools/mongoimport/) stops if major
differences between the actual and expected number of columns. Therefore, my plan
changed and my nadloader.py [python script](https://www.python.org/) with 
the help from [pymongo](https://pypi.org/project/pymongo/) package did import the data. The concern with this approach was
the start and stop completion time. The plan changed again,
with sqlite3. 

[Sqlite](https://sqlite.org/index.html) is a simple and lightweight rational database management system 
available on linux operating systems with download versions available on 
Apple Macs and Microsoft Windows. Sqlite start and stop completion time was about
10 minutes, 30-40 minutes faster the my python script.

```shell
sqlite> .import NAD_r11.txt dump.
```

