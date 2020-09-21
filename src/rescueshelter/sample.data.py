import random
import datetime
import pymongo

# Python 3.7 in use.
# Python 3.6 minimal requirement
#
# PEP 498 introduces a new kind of string literals: f-strings, or formatted string literals.
from hashlib import blake2b # Python 3.6

# https://docs.python.org/3.6/library/multiprocessing.html#module-multiprocessing
from multiprocessing import Pool
from multiprocessing.pool import AsyncResult
from typing import Iterable, Any

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
    utcnow = datetime.datetime.utcnow()
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

# https://docs.python.org/3.7/library/hashlib.html?highlight=blake#hashlib.blake2b
# https://www.npmjs.com/package/blake2
def encryptedData(data, key = 'Rescue Shelter: Security Question Answer') :
    tmpData = data.strip().encode('utf-8')
    tmpKey = key.strip().encode('utf-8')

    fn = blake2b(digest_size=16, key=tmpKey)
    fn.update(tmpData)
    return fn.hexdigest()

def verifyEncryptedData() :
    print('Verify sponsor sample password data')

def loadSponsorTestData() :
    print('Loading sponsor sample data')

    client = pymongo.MongoClient("localhost", 27017)
    db = client.get_database("rescueshelter")

    #db.drop_collection("sponsors")
    col = db.get_collection("sponsors")

    print('Use #P@ssw0rd1. with these available email:')
    for index in range(10) :            
        firstname = ''.join(word_generator(wordTemplate,wordSize))
        lastname = ''.join(word_generator(wordTemplate,wordSize))

        print(f'\t{firstname}.{lastname}@rescueshelter.co')
        col.b
        col.insert_one({
            'firstname': firstname,
            'lastname': lastname,
            'useremail': f'{firstname}.{lastname}@rescueshelter.co',
            'photo': '',
            'security': {
                'password': encryptedData(data='#P@ssw0rd1.', key=f'{firstname}.{lastname}@rescueshelter.co')
            },
            'audit': []
        })

    client.close()

def bulkLoadAnimalTestData(insert_count: int = 100000) :
    print('Loading sample animal data')    

    client = pymongo.MongoClient("localhost", 27017)
    db = client.get_database("rescueshelter")

    # db.drop_collection("animals")    
    col = db.get_collection("animals")

    batches = bulkWriteListSizes(insert_count)
    for batch_size in batches :
        starttime = datetime.datetime.now()
        
        col.bulk_write([
            {
                'insertOne': {            
                    'name': ''.join(word_generator(wordTemplate, wordSize)),
                    'description': description,
                    'image': { 
                        'content': animalImageIconType_choice(animalImageIconTypes),
                        'contenttype': 'icon'
                    },
                    'category': animalCategoryType_choice(animalCategoryTypes),
                    'endangered': endangeredTypes_choice(endangeredTypes),
                    'data': populationData(population_generator(size_min,size_max)),
                    'dates': {
                        'created': datetime.datetime.utcnow(),
                        'modified': datetime.datetime.utcnow()
                    },
                    'sponsors': []
                } 
            }
            for i in range(batch_size)], ordered=False)            
        
        endtime = datetime.datetime.now()
        print(f'Duration: {endtime-starttime}')
    
    client.close()


def loadAnimalTestData(insert_count: int = 100000) :
    print('Loading sample animal data')
    starttime = datetime.datetime.now()

    batches = bulkWriteListSizes(insert_count)

    client = pymongo.MongoClient("localhost", 27017)
    db = client.get_database("rescueshelter")

    # db.drop_collection("animals")
    col = db.get_collection("animals")
    for batch_size in batches :
        col.insert_many([
            {
                'name': ''.join(word_generator(wordTemplate, wordSize)),
                'description': description,
                'image': { 
                    'content': animalImageIconType_choice(animalImageIconTypes),
                    'contenttype': 'icon'
                },
                'category': animalCategoryType_choice(animalCategoryTypes),
                'endangered': endangeredTypes_choice(endangeredTypes),
                'data': populationData(population_generator(size_min,size_max)),
                'dates': {
                    'created': datetime.datetime.utcnow(),
                    'modified': datetime.datetime.utcnow()
                },
                'sponsors': []
            } for i in range(batch_size)])
    
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
    loadAnimalTestData(100000)
    #startMulticoreProcessFor(bulkLoadAnimalTestData, [100000,], 'loading animal test data with bulk write')

if __name__ == "__main__":
    main()