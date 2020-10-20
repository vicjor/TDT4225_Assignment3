from DbConnector import DbConnector
from haversine import haversine
from pprint import pprint 


class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db 

    def query1(self, col_name):
        collection = self.db[col_name]
        users = collection.count_documents({})
        #count_activities = db.Activity.find({}).count()
        print(users)

    def main(self):
        print("User:")
        self.query1(col_name='User')
        print("Activity:")
        self.query1(col_name='Activity')
        print("TrackPoint:")
        self.query1(col_name='TrackPoint')

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