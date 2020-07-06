import random
import json

import uuid
import pymongo

# Simple script that generates random mock Disability and Health Data System sample data
# https://www.cdc.gov/ncbddd/disabilityandhealth/features/disability-health-data.html
#
#
# This script generates a large JSON list batch import, similar with MongoDB or other
# document base data storage, batch import options.
#
# Opinion: This is a better option than a relationship database cursor or store
# procedure approach with transaction on insert statements. 
# 

disabilityTypes_sampler = random.sample #Saves the sample([],k=0) function for use later
disabilityTypes = [ 'cognitive', 'mobility', 'vision', 'self-care', 'independent living', 'hearing' ]
disabilityTypes_len = len(disabilityTypes)

healthTypes_sampler = random.sample #Saves the sample([],k=0) function for use later
healthTypes = [
    'Smoker',
    'Obesity',
    'Heart disease',
    'High blood pressure',
    'HIV',
    'Cancer'
    'Hashimoto',
    'Sicklecell',
    'Sadness',
    'Fatigue',
    'Sleeplessness',
    'Diabetes',
    'Binge drinker',
    'Flu vaccine',
    'Coronavirus (Covid19)',
    'No Healthcare coverage'
]
healthTypes_len = len(healthTypes)


genderTypes_choice = random.choice #Saves the choice([]) function for use later
genderTypes = [ 'female', 'male', 'transwoman', 'transman', 'private' ]

age_generator = random.randint #Saves the randint(min,max) function for use later
age_min = 18
age_max = 70

militaryType_choice = random.choice #Saves the choice([]) function for use later
militaryTypes = [ 'active', 'not active', 'not applicable', 'private' ]

salaryRangeTypes_choice = random.choice #Saves the choice([]) function for use later
salaryRangeTypes = [
    {'minimum':0, 'maximum':19999},
    {'minimum':20000, 'maximum':39999},
    {'minimum':40000, 'maximum':59999},
    {'minimum':60000, 'maximum':79999},
    {'minimum':80000, 'maximum':99999},
    {'minimum':100000, 'maximum':'+'}
]

employmentTypes_choice = random.choice #Save the choice([]) function for use later
employmentTypes = [
    'W2', 
    '1099', #Consultant, Contract, Self, Independent, etc...
    'Unemployed',
    'Other'
]

relationshipStatusTypes_choice = random.choice #Save choice([]) function for use later
relationshipStatusTypes = [
    'Single',
    'Head of House',
    'Homemaker'
    'Married',
    'Partners',
    'Civil Union',
    'Divorced',
    'Widower',
    'Not Applicable'
]

educationTypes_choice = random.choice #Saves the choice([]) function for use later
educationTypes = ['High School Diploma/GED', 'Some College', 'Continuous Education', 'Associate Degree', 'Bachelor Degree', 'Master Degree', 'PhD Degree', 'Certification(s)', 'Other']

class dhdsMockData: 
    def __init__(self):
        super().__init__()

        self._id = str(uuid.uuid4()) #convert to string
        self.disabilityTypes = disabilityTypes_sampler(disabilityTypes,random.randint(0,disabilityTypes_len))
        self.healthTypes = healthTypes_sampler(healthTypes,random.randint(0,healthTypes_len))

        self.gender = genderTypes_choice(genderTypes)
        self.age = age_generator(age_min,age_max)    

        self.hasDisabilities = False
        if len(self.disabilityTypes) > 0:
            self.hasDisabilities = True

        self.educationLevel = educationTypes_choice(educationTypes)
        self.salaryRange = salaryRangeTypes_choice(salaryRangeTypes)
        self.employmentStatus = employmentTypes_choice(employmentTypes)
        self.martialStatus = relationshipStatusTypes_choice(relationshipStatusTypes)

client = pymongo.MongoClient("localhost", 27017)
db = client.get_database("datalake")

col = db.get_collection("dsdh")
col.insert_many([
    {
        '_id': str(uuid.uuid4()),
        'disabilityTypes': disabilityTypes_sampler(disabilityTypes,random.randint(0,disabilityTypes_len)),
        'healthTypes': healthTypes_sampler(healthTypes,random.randint(0,healthTypes_len)),
        'gender': genderTypes_choice(genderTypes),
        'age': age_generator(age_min,age_max),
        'hasDisabilities': False,
        'educationLevel': educationTypes_choice(educationTypes),
        'salaryRange': salaryRangeTypes_choice(salaryRangeTypes),
        'employmentStatus': employmentTypes_choice(employmentTypes),
        'martialStatus': relationshipStatusTypes_choice(relationshipStatusTypes)
    } for i in range(100000)])

col.update_many({
    'disabilityTypes': {
        '$count': {'$gt': 0}}
 }, { 'hasDisabilities': 'true'})

client.close()
