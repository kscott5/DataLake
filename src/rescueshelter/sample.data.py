import random
import datetime
import itertools
import math
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.operations import InsertOne

connectionString = "mongodb://localhost:27017"

# Python 3.7 in use.
# Python 3.6 minimal requirement
#
# PEP 498 introduces a new kind of string literals: f-strings, or formatted string literals.
from hashlib import blake2b # Python 3.6

# https://docs.python.org/3.6/library/multiprocessing.html#module-multiprocessing
from multiprocessing import Pool
from multiprocessing.pool import AsyncResult
from typing import Iterable, Any

SAMPLE_DATA_SIZE = 1000000

# localhost:3302/api/manage/secure/data
# https://github.com/kscott5/rescueshelter.services/src/middleware/DataEncryption.ts
emailPassword = { 
	'plaintext': 'P@$$w0rd1',
	'encrypted': '6b704c1131df59acb475dee5ef1da4d8', 
	'key': 'RS Default Secret Key', 
	'iv': [191, 173, 60, 199, 43, 61, 43, 13, 54, 47, 28, 252, 36, 163, 161, 141]
}

emailDnsTypes = ['gmail.com', 'outlook.com', 'rescueshelter.co', 'yahoo.com']
emailDns_choice = random.choice

word_size_min = 5
word_size_max = 10
wordSize = random.randint(word_size_min, word_size_max)

word_generator = random.sample #Saves the sample([],k=0) function for use later
wordTemplate =  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

animalImageIconType_choice = random.choice
animalImageIconTypes = ['deskpro', 'docker', 'earlybirds', 'drupal', 'firefox',
    'github', 'gitlab', 'grunt', 'linux', 'napster', 'phoenix framework', 'qq',
    'reddit alien', 'snapchat ghost', 'sticker mule', 'twitter', 'tripadvisor',
    'vaadin', 'themeisle', 'github alternate']

animalCategoryType_choice = random.choice
animalCategoryTypes = ['fishes', 'amphibians', 'reptiles', 'birds', 'mammals', 'invertebrates']
animalCategoryTypesDict = [
    {'type':'fishes','percentage':0}, 
    {'type':'amphibians','percentage':0},
    {'type':'reptiles','percentage':0},
    {'type':'birds','percentage':0},
    {'type':'mammals','percentage':0},
    {'type':'invertebrates','percentage':0}
]
animalCategoryTypePerctage = [0.39, 0.28, 0.22, .07, 0.3, 0.1]

endangeredTypes_choice = random.choice #Saves the choice([]) function for use later
endangeredTypes = [ True, False]

size_min = 100
size_max = 500
population_generator = random.randint #Saves the randint(min,max) function for use later

description = '''Lorem ipsum dōlor sit ǽmet, reqūe tation constiÞuto vis eu, est ðōlor omnīum āntiopæm ei. 
                Zril domīng cū eam, hās ið equīðem explīcærī voluptǽtum. Iusto regiōnē partiendo meǣ ne, vim 
                cu ælii āltērum vōlutpāt, vis et aliquip trītæni. Dolor luptātum sapienÞem cu pēr, dico qūæs 
                ðissentiǣs et eūm, vix ut.'''

def populationData(populationToday):
    delta = datetime.timedelta(days=-270)
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    population = populationToday

    data = []
    for i in range(100) :
        if i >= 1 : # entry
            utcnow = utcnow+delta 
            population = random.randint(100, 200)

        data.append({
            'population': population,
            'created': utcnow
        })

    return data

def bulkWriteListSizes(totalWrites: int = 1000000) : 
    CHUCK_SIZE = 1024 # History: Bytes in 1 Kilobyte

    # Total number of inserts in last bulk write operation
    last_batch_size = totalWrites % CHUCK_SIZE

    # Total number of batches for bulk write operation
    batch_count = (totalWrites - (last_batch_size)) // CHUCK_SIZE

    # Initial list with size of each bulk write operation
    results = [CHUCK_SIZE for count in range( batch_count )]

    if last_batch_size > 0 : results.append(last_batch_size)
    if sum(results) == totalWrites : return results

    return [0]

def loadSponsorTestData() :
    print('Loading sponsor sample data')

    client = MongoClient(connectionString)
    db = client.get_database("rescueshelter")

    col = db.get_collection("sponsors")

    print(f'Use {emailPassword.get("plaintext")} with these available email:')
    for sponsor in itertools.combinations(animalCategoryTypes+animalImageIconTypes, 2) :
        firstname = ''.join(sponsor[0]).replace(' ', '')
        lastname = ''.join(sponsor[1])

        domain = emailDns_choice(emailDnsTypes)
        username = firstname.replace(' ','') + '.' + lastname.replace(' ', '')            
        useremail = f'{username}@{domain}'

        print(f'\t{useremail}')

        col.insert_one({
            'firstname': firstname,
            'lastname': lastname,
            'useremail': useremail,
            'username': username,
            'photo': '',
            'security': {
                'password': emailPassword.get('encrypted') 
            },
            'audit': []
        })

    client.close()
    print(f'Use {emailPassword.get("plaintext")} with these available email:')

def initAnimalCategoryTypePercentage():
    for percentage in animalCategoryTypePerctage:
     while(True):
        category = animalCategoryType_choice(animalCategoryTypesDict)
        if category['percentage'] == 0: 
            category['percentage'] = percentage
            break

def bulkLoadAnimalTestData(insert_count: int = 100000) :
    print('Loading sample animal data')    

    client = MongoClient(connectionString)
    db = client.get_database("rescueshelter")

    col = db.get_collection("animals")

    initAnimalCategoryTypePercentage()    
    batches = bulkWriteListSizes(insert_count)
    
    for batch_size in batches :
        starttime = datetime.datetime.now()
        
        for animalCategory in animalCategoryTypesDict:
            category_size = math.ceil(batch_size*animalCategory["percentage"])
            
            col.bulk_write([
                InsertOne({            
                        'name': ''.join(word_generator(wordTemplate, wordSize)),
                        'description': description,
                        'image': { 
                            'content': animalImageIconType_choice(animalImageIconTypes),
                            'contenttype': 'icon'
                        },
                        'category': animalCategory['type'],
                        'endangered': endangeredTypes_choice(endangeredTypes),
                        'data': populationData(population_generator(size_min,size_max)),
                        'dates': {
                            'created': datetime.datetime.now(datetime.timezone.utc),
                            'modified': datetime.datetime.now(datetime.timezone.utc)
                        },
                        'sponsors': []
                }) 
                for i in range(category_size)], ordered=False)   
            
        endtime = datetime.datetime.now()
        print(f'Duration: {endtime-starttime}')
    
    client.close()


def loadAnimalTestData(insert_count: int = 100000) :
    print('Loading sample animal data')
    starttime = datetime.datetime.now()

    batches = bulkWriteListSizes(insert_count)
    initAnimalCategoryTypePercentage()

    client = MongoClient(connectionString)
    db = client.get_database("rescueshelter")

    col = db.get_collection("animals")
    for batch_size in batches :
        for animalCategory in animalCategoryTypesDict:
            category_size = math.ceil(batch_size*animalCategory["percentage"])
        
            col.insert_many([
                {
                    'name': ''.join(word_generator(wordTemplate, wordSize)),
                    'description': description,
                    'image': { 
                        'content': animalImageIconType_choice(animalImageIconTypes),
                        'contenttype': 'icon'
                    },
                    'category': animalCategory['type'],
                    'endangered': endangeredTypes_choice(endangeredTypes),
                    'data': populationData(population_generator(size_min,size_max)),
                    'dates': {
                        'created': datetime.datetime.now(datetime.timezone.utc),
                        'modified': datetime.datetime.now(datetime.timezone.utc)
                    },
                    'sponsors': []
                } for i in range(category_size)], ordered=False)
    
    client.close()

    endtime = datetime.datetime.now()
    print(f'Duration: {endtime-starttime}')

def startMulticoreProcessFor(function, args: Iterable[Any], message  = 'Multiple core processor function evaluation') :
    print(message)
    starttime = datetime.datetime.now()

    with Pool(processes=4) as pool :         
        # launching multiple evaluations asynchronously *may* use more processes
        asyncResults = [pool.apply_async(func=function, args=args) for i in range(4)]
        
        while True :
            # Create a list of asyncResults ready state values
            if False in [asyncResult.ready() for asyncResult in asyncResults] : continue # One function execution is not complete
            else : break

    endtime = datetime.datetime.now()    
    print(f'Duration: {endtime-starttime}')


def main() :
    loadSponsorTestData()
    loadAnimalTestData(SAMPLE_DATA_SIZE) #oops not 100 Million, 100000000
    #startMulticoreProcessFor(bulkLoadAnimalTestData, [100000,], 'loading animal test data with bulk write')

if __name__ == "__main__":
    main()