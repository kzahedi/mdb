# %% imports

from pymongo import MongoClient
from multiprocess import Process
import uuid
import random
import time
import numpy as np


def create_random_doc():
    doc = {"_id": str(uuid.uuid4())}
    for i in range(0, 10):
        name = "field " + str(i)
        value = random.randint(0, 1000)
        doc[name] = value
    return doc


def create_list_of_documents(nr):
    lst = []
    for _ in range(0, nr):
        lst.append(create_random_doc())
    return lst


def add_documents_to_collection(collection, docs):
    for doc in docs:
        collection.insert_one(doc)


if __name__ == "__main__":
    singles = []
    two_one = []
    two_two = []

    docs1 = create_list_of_documents(50000)
    docs2 = create_list_of_documents(50000)

    for _ in range(0, 3):

        db_name = "TestMongoDB"

        client = MongoClient()
        db_names = client.list_database_names()

        if db_name in db_names:
            print("%s exists. Will delete" % (db_name))
            client.drop_database(db_name)

        db = client[db_name]

        # %% timing single insert
        print("Single upload test")
        single_test = db["single_test"]
        start = time.time()
        add_documents_to_collection(single_test, docs1)
        end = time.time()
        print("time %f seconds" % (end - start))
        singles.append(end - start)

        print("Two processes, one collections")

        double_test_one_collection_first = db["two_and_one"]
        double_test_one_collection_second = db["two_and_one"]

        def insert_first(docs):
            add_documents_to_collection(double_test_one_collection_first, docs)

        def insert_second(docs):
            add_documents_to_collection(double_test_one_collection_second, docs)

        p1 = Process(target=insert_first, args=(docs1,))
        p2 = Process(target=insert_second, args=(docs2,))

        start = time.time()
        p1.start()
        p2.start()
        p1.join()
        p2.join()
        end = time.time()
        print("time %f seconds" % (end - start))
        two_one.append(end - start)

        print("Two processes, two collections")

        double_test_double_collection_first = db["two_and_two_one"]
        double_test_double_collection_second = db["two_and_two_two"]

        def insert_first(docs):
            add_documents_to_collection(double_test_double_collection_first, docs)

        def insert_second(docs):
            add_documents_to_collection(double_test_double_collection_second, docs)

        p1 = Process(target=insert_first, args=(docs1,))
        p2 = Process(target=insert_second, args=(docs2,))

        start = time.time()
        p1.start()
        p2.start()
        p1.join()
        p2.join()
        end = time.time()
        print("time %f seconds" % (end - start))
        two_two.append(end - start)

print("Single %f" % (np.mean(singles)))
print("Two-one %f" % (np.mean(two_one)))
print("Two-two %f" % (np.mean(two_two)))


# %%

# %%
