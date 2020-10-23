from unittest.case import _id

from DbConnector import DbConnector
from haversine import haversine
from pprint import pprint


class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.activity = self.db['Activity']
        self.user = self.db['User']
        self.trackpoint = self.db['TrackPoint']


    def query1(self, collection):
        count = collection.count_documents({})
        #count_activities = db.Activity.find({}).count()
        print(count)

    def query2(self):
        activity = self.activity.count_documents({})
        user = self.user.count_documents({})
        avg = activity / user
        print(avg)

    def query3(self):
        query = self.activity.aggregate([
            {'$group': {'_id': '$user_id',
                'count': { '$sum': 1}
                }
            },
            {'$sort': {'count':-1}},
            {'$limit': 20}
        ])
        for item in query:
            pprint(item)

    def query4(self): #får ut riktige id'er, men ikke sortert riktig - fikser hvis tid
        query = self.activity.aggregate([
            {'$match': {'transportation_mode': 'taxi'}},
            {'$group': {'_id': '$user_id'}},
            {'$sort': {'_id':1}}
        ])
        for item in query:
            pprint(item)

    def query5(self):
        query = self.activity.aggregate([
            {'$group': {'_id': '$transportation_mode', 'count': {'$sum':1}}},
            {'$sort': {'_id':1}},
            {'$skip': 1}])
        for item in query:
            pprint(item)

    def query6a(self):
        query = self.activity.aggregate([
            {'$project': {'year': {'$year' : '$start_date_time'}}},
            {'$group': {'_id': '$year', 'count':{'$sum':1}}},
            {'$sort':{'count':-1}},
            {'$limit':1}
        ])
        for item in query:
            pprint(item)

    #def query6b(self):

    def query8(self):
        query = self.activity.aggregate([
            {
                '$lookup': {
                    'from': 'TrackPoint',
                    'localField': '_id',
                    'foreignField': 'activity_id',
                    'as': 'trackpoints'
                }
            },
            {
                '$project': {
                    '_id': '$_id',
                    'user_id': '$user_id',
                    'trackpoints.altitude': '$trackpoints.altitude',

                }
            }])
        print('Query 8: highest total altitude: ')

        for item in query:
            pprint(item)

    def query9(self):
        query = self.activity.aggregate([
            {
                '$lookup': {
                    'from': 'TrackPoint',
                    'localField': '_id',
                    'foreignField': 'activity_id',
                    'as': 'trackpoints'
                }
            },
            {
                '$project': {
                    'trackpoints.date_days': '$trackpoints.date_days',
                    'user_id': '$user_id',
                    '_id': '$_id'
                }
            }])


        print('Query 9: Total number of invalid activities: ')

        for user in query:
            pprint(user)

    def query10(self):
        query = self.trackpoint.aggregate([
            {
                '$match': {'$expr': {'$and': [
                    {'$eq': [{'$round': ['$lat', 3]}, 39.916]},
                    {'$eq': [{'$round': ['$lon', 3]}, 116.397]}
                    ]
                    }
                }
            },
            {
                '$lookup': {
                    'from': 'Activity',
                    'localField': 'activity_id',
                    'foreignField': '_id',
                    'as': 'forbiddenCity'
                }
            },
            {'$group': {'_id': '$forbiddenCity.user_id'},},

            {'$project': {'forbiddenCity.user_id': 1}}
        ])

        print("Query 10: Users who have tracked an activity in the forbidden city of Beijing")
        for item in query:
            pprint(item)

    def query11(self):

        query = self.activity.aggregate([
            {'$match': {'transportation_mode': {'$exists': True,'$ne': 'null'}}},

            {'$group': {'_id': {'category': '$transportation_mode','user': '$user_id'},
                        'count': {'$sum': 1},}},

            {'$sort': {'count': -1,'_id.category': -1}},

            {'$group': {'_id': '$_id.user', 'count': {'$first': '$count'},'category': {'$first': '$_id.category'}}},

            {'$project': {'_id': '$_id', 'category': '$category','count': '$count',}},

            {'$sort': {'_id': 1}}
        ])
        print('Query 11: Most used transportation mode per user')
        print('Format: [user_id, transportation_mode, times used]')

        for item in query:
            pprint(item)

    def main(self):
        #print('User:')
        #self.query1(self.user)
        #print('Activity:')
        #self.query1(self.activity)
        #print('TrackPoint:')
        #self.query1(self.trackpoint)
        #self.query2()
        #self.query3()
        #self.query4()
        #self.query5()
        #self.query6a()
        #self.query8()
        self.query9()
        #self.query10()
        #self.query11()

if __name__ == '__main__':
    try:
        qo = Queries()
        qo.main()
    except Exception as e:
        print(e)


'''
def main():
    program = None
    try:
        program = Queries()
        program.query1()
    except Exception as e:
        print('ERROR: Failed to use database:', e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
'''
