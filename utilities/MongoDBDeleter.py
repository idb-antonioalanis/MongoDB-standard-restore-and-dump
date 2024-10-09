from pymongo import MongoClient


class MongoDBDeleter:
    """
    This class is responsible for deleting a certain quantity of documents from a MongoDB collection.
    """

    def __init__(self):
        self.DEFAULT_CONNECTION_STRING = "mongodb://localhost:27017/"
        self.BATCH_SIZE = (
            1000  # `delete_many` method has a limit of 1000 documents per call.
        )

        self._invoke()

    def _get_input(self):
        """
        Get the necessary input from the user. Calculate the quantity of documents to delete.
        """

        self.connection_string = (
            input(
                f"MongoDB connection URI (default: {self.DEFAULT_CONNECTION_STRING}) > "
            )
            or self.DEFAULT_CONNECTION_STRING
        )
        self.db_name = input("Database name > ")
        self.collection_name = input("Collection name > ")
        self.total_quantity = int(input("Total number of documents > "))
        self.quantity_to_preserve = int(input("Number of documents to preserve > "))

        self.quantity_to_delete = self.total_quantity - self.quantity_to_preserve

    def _connect_db(self):
        """
        Connect to the MongoDB database and get the collection.
        """
        print(f"\n[Info] Connecting to '{self.connection_string}'...")

        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

        print("[Success] Connected to the database.\n")

    def _set_ids_to_delete(self):
        """
        Get the IDs of the documents to delete.
        """
        cursor = self.collection.find({}, {"_id": 1}).limit(self.quantity_to_delete)
        self.ids_to_delete = [document["_id"] for document in cursor]

    def _delete_batches(self):
        """
        Delete the documents in batches.
        """
        batches = [
            self.ids_to_delete[index : index + self.BATCH_SIZE]
            for index in range(0, len(self.ids_to_delete), self.BATCH_SIZE)
        ]

        deleted_count = 0

        for batch in batches:
            result = self.collection.delete_many({"_id": {"$in": batch}})
            deleted_count += result.deleted_count

            print(
                f"[Info] {result.deleted_count} documents deleted on this batch. {deleted_count} documents deleted."
            )

        print(f"\n[Success] {self.quantity_to_preserve} documents preserved.")

    def _invoke(self):
        """
        When the class is instantiated, this method is called to start its execution.
        """
        self._get_input()
        self._connect_db()
        self._set_ids_to_delete()
        self._delete_batches()

        self.client.close()
        print("[Info] Connection to the database closed.")


if __name__ == "__main__":
    MongoDBDeleter()
