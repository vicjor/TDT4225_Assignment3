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

    #def query6a(self):

    #def query6b(self):

    def main(self):
        #print("User:")
        #self.query1(self.user)
        #print("Activity:")
        #self.query1(self.activity)
        #print("TrackPoint:")
        #self.query1(self.trackpoint)
        #self.query2()
        #self.query3()
        #self.query4()
        #self.query5()

if __name__ == "__main__":
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
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
'''