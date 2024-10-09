#!/bin/bash

URI="mongodb://localhost:27017/"  # default MongoDB URI

function show_help() {
    # Show script usage.

    echo
    echo "[Usage]"
    echo
    echo "$0 -u <uri> -d <db> -c <collection>"
    echo
    echo "Parameters"
    echo "  -u   MongoDB URI (default: mongodb://localhost:27017/)"
    echo "  -d   Database to back up"
    echo "  -c   Collection to back up"
    echo
    echo "[Example]"
    echo
    echo "$0 -u 'mongodb://localhost:27017/' -d 'mydatabase' -c 'mycollection'"
}

function assign_parameters() {
    # Assign parameters to variables. If a parameter is not provided, use the default value or show an 
    # error message.

    if [[ $# -lt 4 ]]; then
        show_help
        exit 1
    fi

    while getopts "u:d:c:" opt; do
        case $opt in
            u) URI=$OPTARG ;;
            d) DB=$OPTARG ;;
            c) COLLECTION=$OPTARG ;;
            *) show_help
               exit 1 ;;
        esac
    done
}

function install_dependencies() {
    # Install MongoDB tools and mongosh if they are not installed.

    if ! command -v mongosh &> /dev/null; then
        echo "[Info] mongosh is not installed. Proceeding with installation..."
        echo

        sudo apt update
        sudo apt install -y gnupg
        wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -
        echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
        sudo apt update
        sudo apt install -y mongodb-mongosh

        echo
        echo "[Info] mongosh has been installed."
    else
        echo "[Info] mongosh is already installed."
    fi

    if ! mongorestore --version &> /dev/null; then
        echo "[Info] mongodb-org-tools is not installed. Proceeding with installation..."
        echo

        wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -
        echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
        sudo apt update
        sudo apt install -y mongodb-org-tools
        mongorestore --version

        echo
        echo "[Info] mongodb-org-tools has been installed."
    else
        echo "[Info] mongodb-org-tools is already installed."
    fi
}

function check_connection() {
    # Check if the script can connect to the MongoDB instance.

    echo
    echo "[Info] Connecting to '$URI'..."

    if ! mongosh "$URI" --quiet --eval "try { db.serverStatus(); } catch(e) { print(e); }" | grep -q "error"; then
        echo "[Error] No online database is found. Could not connect to MongoDB."
        exit 1
    fi

    echo "[Success] Connected to the database."
}

function check_db_and_collection() {
    # Check if the database and collection exist in MongoDB.

    local db=$1
    local collection=$2

    echo
    echo "[Info] Checking if database '$db' and collection '$collection' exist..."

    if ! mongosh "$URI" --quiet --eval "db.getMongo().getDBNames().indexOf('$db') > -1" | grep -q "true"; then
        echo "[Error] Database '$db' does not exist."
        exit 1
    fi

    if ! mongosh "$URI" --quiet --eval "db.getSiblingDB('$db').getCollectionNames().indexOf('$collection') > -1" | grep -q "true"; then
        echo "[Error] Collection '$collection' does not exist in database '$db'."
        exit 1
    fi

    echo "[Info] Database '$db' and collection '$collection' exist."
}

function generate_dump() {
    # Generate a dump of the specified collection.

    echo
    echo "[Info] Generating backup for '$DB.$COLLECTION' in 'data/dump'..."
    echo

    mongodump --uri="$URI" --db="$DB" --collection="$COLLECTION" --gzip --out=data/dump

    echo

    if [[ $? -ne 0 ]]; then
        echo "[Error] Failed to generate the backup. Please check the MongoDB logs for details."
        exit 1
    else
        echo "[Success] Backup completed successfully. Dump saved to 'data/dump'."
    fi
}

function main() {
    # Main function to execute the script.

    assign_parameters "$@"
    install_dependencies
    check_connection
    check_db_and_collection "$DB" "$COLLECTION"
    generate_dump
}

main "$@"
