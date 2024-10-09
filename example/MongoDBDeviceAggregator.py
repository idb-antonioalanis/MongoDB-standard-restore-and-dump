from pymongo import MongoClient, ASCENDING

# {"_id":"2AC4BC3936037F9EBB14FD07607332E661E1480D","hgus":[{"id":"locust-seq-00004401","brand":"movistar","ip":"0.0.17.49","identities":[{"type":"phone_number","value":"+34900017049"}],"firmwareVersion":"ES_g19.X_R3505VWSTD203_dev_r8386","uptime":33701536,"devices":[{"lastDisconnectionTime":1627452927530,"interfaceName":"wifi0","dhcp60":"android-dhcp-8.1.0","hostName":"a733b1424165fb6d2df10e02ff48666f","connected":false,"id":"90ab017c-4783-4787-afdd-2643d6dd1e9f","mac":"fa:ba:da:00:00:00","ip":"192.168.1.33","viewed":true,"firstConnectionTime":1652796080934,"deviceName":null,"deviceCategory":null,"deviceType":"generic","deviceModel":null,"deviceManufacturer":null,"deviceOs":null,"deviceOsVersion":null,"deviceAdditionalInfo":null,"nickName":null,"nickCategory":null,"paused":false,"dhcp55":"FzZKaMPYMPYdXJYSGYLE"},{"lastDisconnectionTime":1627452927530,"interfaceName":"wifi0","dhcp60":"android-dhcp-8.1.0","hostName":"1a40e7eeeffb158382df084ee3eb5f29","connected":false,"id":"ecdf3dea-e88a-4321-a643-78e9c9ff9e53","mac":"fa:ba:da:00:00:01","ip":"192.168.1.33","viewed":true,"firstConnectionTime":1652796080934,"deviceName":null,"deviceCategory":null,"deviceType":"generic","deviceModel":null,"deviceManufacturer":null,"deviceOs":null,"deviceOsVersion":null,"deviceAdditionalInfo":null,"nickName":null,"nickCategory":null,"paused":false,"dhcp55":"FzZKaMPYMPYdXJYSGYLE"},{"lastDisconnectionTime":1627452927530,"interfaceName":"wifi0","dhcp60":"android-dhcp-8.1.0","hostName":"da85de81550d688b9083c7f713657116","connected":true,"id":"a69ba8bf-65cd-4ac4-b4c9-d6b884188964","mac":"fa:ba:da:00:00:02","ip":"192.168.1.33","viewed":true,"firstConnectionTime":1652796080934,"deviceName":null,"deviceCategory":null,"deviceType":"generic","deviceModel":null,"deviceManufacturer":null,"deviceOs":null,"deviceOsVersion":null,"deviceAdditionalInfo":null,"nickName":null,"nickCategory":null,"paused":false,"dhcp55":"FzZKaMPYMPYdXJYSGYLE"}],"smartWifiMigrationStatus":null,"iotHubName":"iothub"}],"updatedAt":"2022-05-30T12:57:37.417Z","createdAt":"2022-03-02T08:27:47.416Z"}


class MongoDBDeviceAggregator:
    """
    This class handles aggregating data from a MongoDB collection, transforming it, and inserting the results into another collection.
    """

    def __init__(self):
        self.DEFAULT_CONNECTION_STRING = "mongodb://localhost:27017/"
        self.DEFAULT_DB_NAME = "haac"
        self.DEFAULT_COLLECTION_NAME = "user-hgus"
        self.DEFAULT_NEW_DB_NAME = "datasets"
        self.DEFAULT_NEW_COLLECTION_NAME = "stations_catalogue"

        self._invoke()

    def _get_input(self):
        """
        Get necessary inputs from the user for database and collection names, using defaults if none provided.
        """
        self.connection_string = (
            input(
                f"Enter connection URI (default: {self.DEFAULT_CONNECTION_STRING}) > "
            )
            or self.DEFAULT_CONNECTION_STRING
        )
        self.db_name = (
            input(
                f"Enter the source database name (default: {self.DEFAULT_DB_NAME}) > "
            )
            or self.DEFAULT_DB_NAME
        )
        self.collection_name = (
            input(
                f"Enter the source collection name (default: {self.DEFAULT_COLLECTION_NAME}) > "
            )
            or self.DEFAULT_COLLECTION_NAME
        )
        self.new_db_name = (
            input(
                f"Enter the target database name (default: {self.DEFAULT_NEW_DB_NAME}) > "
            )
            or self.DEFAULT_NEW_DB_NAME
        )
        self.new_collection_name = (
            input(
                f"Enter the target collection name (default: {self.DEFAULT_NEW_COLLECTION_NAME}) > "
            )
            or self.DEFAULT_NEW_COLLECTION_NAME
        )

    def _connect_db(self):
        """
        Connect to the MongoDB database and collections.
        """
        self.client = MongoClient(self.connection_string)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

        self.new_db = self.client[self.new_db_name]
        self.new_collection = self.new_db[self.new_collection_name]

    def _count_devices(self):
        """
        Count the number of devices in the source collection.
        """
        devices_count = self.collection.aggregate(
            [{"$unwind": "$hgus"}, {"$unwind": "$hgus.devices"}, {"$count": "devices"}]
        ).next()["devices"]
        print(f"{devices_count} devices found.")

    def _aggregate_and_insert(self):
        """
        Perform aggregation on the source collection and insert the results into the target collection.
        """
        self.collection.aggregate(
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
                            "db": self.new_db_name,
                            "coll": self.new_collection_name,
                        },
                        "whenMatched": "merge",
                        "whenNotMatched": "insert",
                    }
                },
            ]
        )
        print(f"Data inserted in {self.new_collection_name} collection.")

    def _create_index(self):
        """
        Create an index on the target collection to optimize queries.
        """
        self.new_collection.create_index(
            [("USER_4P_ID", ASCENDING), ("PHONE_WITH_PREFIX_ID", ASCENDING)],
            unique=False,  # allow duplicates
        )
        print("Index created.")

    def _invoke(self):
        """
        When the class is instantiated, this method is called to start its execution.
        """
        self._get_input()
        self._connect_db()
        self._count_devices()
        self._aggregate_and_insert()
        self._create_index()

        self.client.close()


if __name__ == "__main__":
    MongoDBDeviceAggregator()
