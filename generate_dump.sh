#!/bin/bash


URI="mongodb://localhost:27017/"  # default MongoDB URI


function show_help() {
    # Show script usage.

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
    echo
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
    # Install MongoDB tools if they are not installed.

    if ! mongorestore --version &> /dev/null; then
        echo "[Info] mongodb-org-tools is not installed. Proceeding with installation..."
        echo

        sudo apt update
        sudo apt install -y gnupg

        wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -
        echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list

        sudo apt update
        sudo apt install -y mongodb-org-tools
        mongorestore --version

        echo "[Info] mongodb-org-tools has been installed."
        echo
    else
        echo "[Info] mongodb-org-tools is already installed."
        echo
    fi
}

function generate_dump() {
    # Generate a dump of the specified collection.

    mongodump --uri="$URI" --db="$DB" --collection="$COLLECTION" --gzip --out=data/dump
}

function main() {
    # Main function to execute the script.

    echo

    assign_parameters "$@"
    install_dependencies
    generate_dump
}

main "$@"