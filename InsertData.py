from typing import Collection
from DbConnector import DbConnector
from tabulate import tabulate
import os
from pprint import pprint
from dataset import dataset
from datetime import datetime
import json
# import _pickle as pickle
import datetime


class GeolifeProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db
        # self.cursor = self.connection.cursor
        self.user_ids = {}
        self.labeled_ids = []
        self.activity_data = {}
        self.trackpoints = []
        self.labeled_data = {}
        self.transportation_modes = ['walk', 'taxi', 'car', 'airplane', 'bike', 'subway', 'bus', 'train', 'other']

    def load_labeled_ids(self):
        f = open('./dataset/dataset/labeled_ids.txt', 'r')
        for labeled_id in f:
            self.labeled_ids.append(labeled_id.split('\n')[0])

    def generate_user_ids(self, source_folder):
        for root, dirs, files in os.walk(source_folder, topdown=True):
            ids = []
            for name in dirs:
                if name != "Trajectory" and name != "Data":
                    ids.append(name)
            ids.sort()
            for id in ids:
                has_label = False
                if id in self.labeled_ids:
                    has_label = True
                self.user_ids[id] = has_label

    def print_user_ids(self):
        for pair in self.user_ids.items():
            print(pair)

    def load_activity_data_from_json(self):
        with open('activity_data.json') as json_file:
            print("Loading data...")
            self.activity_data = json.load(json_file)
            print("Finished loading data from JSON file")

    def insert_users(self):
        Collection = self.db['User']
        # pprint(self.user_ids)
        for user in self.user_ids.items():
            dictionary = {} # Must use dictionaries to insert documents in collection
            dictionary["_id"] = user[0]
            dictionary["has_labels"] = user[1]
            Collection.insert_one(dictionary)
            print(user[0], "/ 181")
        print("========================")
        print("Finished inserting users")

    def insert_activities(self):
        Collection = self.db['Activity']
        _id = 0
        count = 0
        for user_id, activity_list in self.activity_data.items():
            print("Queries finished: " + str(count) + "/181")
            count += 1
            if(len(activity_list) > 0):
                for counter, activity in enumerate(activity_list):
                    _id += 1
                    dictionary = {}
                    dictionary["user_id"] = user_id
                    dictionary["_id"] = _id
                    if(activity[0][2] != "NULL"):
                        dictionary["transportation_mode"] = activity[0][2]
                    dictionary["start_date_time"] = datetime.datetime.strptime(activity[0][5] + " " + activity[0][6], '%Y-%m-%d %H:%M:%S')
                    dictionary["end_date_time"] = datetime.datetime.strptime(activity[-1][5] + " " + activity[-1][6],
                                                                      '%Y-%m-%d %H:%M:%S')
                    Collection.insert_one(dictionary)
                    print(counter, "/", len(activity_list))
        print("=============================")
        print("Finished inserting activities")




    def insert_trackpoints(self):
        Collection = self.db['TrackPoint']

        _id = 16049
        count = "000"

        activity_counter = 0
        for user_id, activity_list in self.activity_data.items():
            print("User ", count, " of", "181")
            count = str(int(count) + 1)

            trackpoints = []

            for activity in activity_list:
                activity_counter += 1
                act_lst = []
                for tp in activity:
                    act = {"_id": _id, "lat": tp[0], "lon": tp[1], "altitude": int(tp[3]), "date_days":tp[4], "date_time": str(tp[5] + " " + tp[6]), "activity_id": activity_counter}
                    # act["_id"] = _id
                    # act["lat"] = tp[0]
                    # act["lon"] = tp[1]
                    # act["altitude"] = int(tp[3])
                    # act["date_days"] = tp[4]
                    # act["date_time"] = str(tp[5] + " " + tp[6])

                    act_lst.append(act)
                    _id += 1
                trackpoints += act_lst
            if (len(trackpoints) > 0):
                Collection.insert_many(trackpoints)
        print("Finished inserting trackpoints")

def main():
    program = None
    try:
        program = GeolifeProgram()  # Init program
        print("Loading labeled ids...")
        program.load_labeled_ids()  # Create list of labeled user ids
        # # Traverse directory and store all user ids in dict with true/false labeled_id
        print("Generating user_ids ids...")
        program.generate_user_ids("./dataset/dataset")
        # program.print_user_ids()  # Control method to check data is correct ex ('000', False)
        print("Loading from JSON...")
        program.load_activity_data_from_json()

        # print("Inserting users to DB")
        # program.insert_users()

        # print("Inserting activities to DB")
        # program.insert_activities()
        print("Inserting trackpoints...")
        program.insert_trackpoints()

        # print(program.activity_data.get("010"))
        # program.create_activity_table()
        # program.create_trackpoint_table()

    except Exception as e:
        print("Error", e)


if __name__ == '__main__':
    main()