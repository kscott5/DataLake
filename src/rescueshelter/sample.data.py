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
import typing

def startMulticoreProcessFor(function, args: typing.Iterable[typing.Any], message  = 'Multiple core processor function evaluation') :
    print(message)
    starttime = datetime.datetime.now()

    with Pool(processes=4) as pool :         
        # launching multiple evaluations asynchronously *may* use more processes
        asyncResults = [pool.apply_async(func=function, args=args) for i in range(4)]
        
        while True :
            # Create a list of asyncResults ready state values
            if False in [asyncResult.ready() for asyncResult in asyncResults] : continue # A function execution not complete
            else : break

    endtime = datetime.datetime.now()    
    print(f'Duration: {endtime-starttime}')

word_size_min = 5
word_size_max = 10
wordSize = random.randint(word_size_min, word_size_max)

word_generator = random.sample #Saves the sample([],k=0) function for use later
wordTemplate =  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

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


def loadAnimalTestData(insert_count=100000) :
    print('Loading sample animal data')
    starttime = datetime.datetime.now()

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

    client = pymongo.MongoClient("localhost", 27017)
    db = client.get_database("rescueshelter")

    # db.drop_collection("animals")
    col = db.get_collection("animals")
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
        } for i in range(insert_count)])
    
    client.close()

    endtime = datetime.datetime.now()
    print(f'Duration: {endtime-starttime}')

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

def main() :
    loadSponsorTestData()
    #loadAnimalTestData(100000)
    startMulticoreProcessFor(loadAnimalTestData, [50000,], 'loading animal test data')

if __name__ == "__main__":
    main()