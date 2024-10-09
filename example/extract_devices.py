from pymongo import MongoClient

DEFAULT_CONNECTION_STRING = "mongodb://localhost:27017/"
DEFAULT_DB_NAME = "datasets"
DEFAULT_COLLECTION_NAME = "user_hgus_10"
DEFAULT_NEW_DB_NAME = "devices"
DEFAULT_NEW_COLLECTION_NAME = "devices"


def get_input():
    """
    Get necessary inputs from the user for database and collection names, using defaults if none provided.
    """
    connection_string = (
        input(f"Enter connection URI (default: {DEFAULT_CONNECTION_STRING}) > ")
        or DEFAULT_CONNECTION_STRING
    )
    db_name = (
        input(f"Enter the source database name (default: {DEFAULT_DB_NAME}) > ")
        or DEFAULT_DB_NAME
    )
    collection_name = (
        input(
            f"Enter the source collection name (default: {DEFAULT_COLLECTION_NAME}) > "
        )
        or DEFAULT_COLLECTION_NAME
    )
    new_db_name = (
        input(f"Enter the target database name (default: {DEFAULT_NEW_DB_NAME}) > ")
        or DEFAULT_NEW_DB_NAME
    )
    new_collection_name = (
        input(
            f"Enter the target collection name (default: {DEFAULT_NEW_COLLECTION_NAME}) > "
        )
        or DEFAULT_NEW_COLLECTION_NAME
    )

    return connection_string, db_name, collection_name, new_db_name, new_collection_name


def connect_db(
    connection_string, db_name, collection_name, new_db_name, new_collection_name
):
    """
    Connect to the MongoDB database and collections.
    """
    client = MongoClient(connection_string)
    db = client[db_name]
    collection = db[collection_name]

    new_db = client[new_db_name]
    new_collection = new_db[new_collection_name]

    return client, collection, new_collection


def count_devices(collection):
    """
    Count the number of devices in the source collection.
    """
    devices_count = collection.aggregate(
        [{"$unwind": "$hgus"}, {"$unwind": "$hgus.devices"}, {"$count": "devices"}]
    ).next()["devices"]
    print(f"{devices_count} devices found.")


def aggregate_and_insert_devices(collection, new_db_name, new_collection_name):
    """
    Aggregate the devices in the source collection and insert them into the target collection.
    """
    collection.aggregate(
        [
            {"$unwind": "$hgus"},
            {"$unwind": "$hgus.devices"},
            {
                "$project": {
                    "_id": {
                        "$concat": [
                            "$_id",
                            "-",
                            {"$toUpper": "$hgus.devices.mac"},
                        ]
                    },
                    "USER_4P_ID": "$_id",
                    "ASSOC_MAC_DES": {"$toUpper": "$hgus.devices.mac"},
                    "DEVICE_ID": "$hgus.id",
                    "PHONE_WITH_PREFIX_ID": {
                        "$arrayElemAt": ["$hgus.identities.value", 0]
                    },
                    "STATION_OS_DES": None,
                    "STATION_OS_KERNEL_DES": None,
                    "STATION_MODEL_DES": "Random Model",
                    "STATION_MODEL_VERSION_DES": "Random Model Version",
                    "STATION_BRAND_DES": "Random Brand",
                    "STATION_TYPE_CD": {
                        "$arrayElemAt": [
                            [
                                "Wireless Bridge",
                                "Network Equipment",
                                "Router",
                                "WiFi Extender",
                                "Gaming Console",
                                "Raspberry",
                                "Smartphone",
                                "PC",
                                "Video Doorbell",
                                "Smart Air Ventilator",
                                "Smart Bulb",
                                "Smart Plug",
                                "eBook",
                                "Printer",
                                "Smart Scale",
                                "Tablet",
                                "TV Dongle",
                                "Smartwatch",
                                None,
                            ],
                            {"$floor": {"$multiply": [{"$rand": {}}, 18]}},
                        ]
                    },
                    "STATION_MAC_VENDOR_DES": "Random Vendor",
                    "STATION_RANDOM_MAC_IND": {
                        "$cond": {
                            "if": {"$gte": [{"$rand": {}}, 0.5]},
                            "then": True,
                            "else": False,
                        }
                    },
                }
            },
            {
                "$merge": {
                    "into": {
                        "db": new_db_name,
                        "coll": new_collection_name,
                    },
                    "whenMatched": "merge",
                    "whenNotMatched": "insert",
                }
            },
        ]
    )
    print(f"Data inserted in {new_collection_name} collection.")


if __name__ == "__main__":
    connection_string, db_name, collection_name, new_db_name, new_collection_name = (
        get_input()
    )

    client, collection, new_collection = connect_db(
        connection_string, db_name, collection_name, new_db_name, new_collection_name
    )

    count_devices(collection)
    aggregate_and_insert_devices(collection, new_db_name, new_collection_name)

    client.close()
