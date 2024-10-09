#!/bin/bash

URI="mongodb://localhost:27017/"  # default MongoDB URI
RESTORE_INDEXES="no" # default Restore indexes option

function show_help() {
    # Show script usage.

    echo
    echo "[Usage]"
    echo
    echo "$0 -u <uri> -d <db> -p <path> -i <yes|no>"
    echo
    echo "Parameters"
    echo "  -u   MongoDB URI (default: mongodb://localhost:27017/)"
    echo "  -d   Database to restore (required)"
    echo "  -p   Path to the dump directory (required)"
    echo "  -i   Restore indexes [yes|no] (default: no)"
    echo
    echo "[Example]"
    echo
    echo "$0 -u 'mongodb://localhost:27018/' -d 'mydatabase' -p '/path/to/dump' -i 'yes'"
}

function assign_parameters() {
    # Assign parameters to variables. If a parameter is not provided, use the default value or show an error message.

    while getopts "u:d:p:i:" opt; do
        case $opt in
            u) URI=$OPTARG ;;
            d) DB=$OPTARG ;;
            p) DUMP_PATH=$OPTARG ;;
            i) 
                if [[ "$OPTARG" != "yes" && "$OPTARG" != "no" ]]; then
                    echo "[Error] -i option must be 'yes' or 'no'."
                    echo
                    show_help
                    exit 1
                fi
                RESTORE_INDEXES=$OPTARG ;;
            *) show_help
               exit 1 ;;
        esac
    done

    if [[ -z "$DB" || -z "$DUMP_PATH" ]]; then
        echo "[Error] Missing required parameters."
        show_help
        exit 1
    fi
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

function restore_dump() {
    # Restore the dump to the specified database.
    
    echo
    echo "[Info] Restoring the dump from '$DUMP_PATH' to database '$DB'..."
    echo

    if [ "$RESTORE_INDEXES" == "yes" ]; then
        mongorestore --uri="$URI" --db="$DB" --gzip "$DUMP_PATH" --drop
    else
        mongorestore --uri="$URI" --db="$DB" --noIndexRestore --gzip "$DUMP_PATH" --drop
    fi

    echo

    if [[ $? -ne 0 ]]; then
        echo "[Error] Failed to restore the database. Please check the MongoDB logs for details."
        exit 1
    else
        echo "[Success] Database restore completed successfully."
    fi
}

function main() {
    # Main function to execute the script.

    assign_parameters "$@"
    install_dependencies
    check_connection
    restore_dump
}

main "$@"
