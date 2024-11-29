# DATABASE PROJECT - SOCIAL MEDIA ANALYTIC TOOL

A place to share mongodb app code

### Setup a python virtual env

```
# If pip is not present in you system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env (Linux/MacOS)
python3 -m pip install virtualenv
python3 -m venv ./venv
source ./venv/bin/activate

# Install and activate virtual env (Windows)
python3 -m pip install virtualenv
python3 -m venv ./venv
.\venv\Scripts\Activate.ps1

# Install project python requirements
pip install -r requirements.txt
```

### To run the script

Ensure you have a running mongodb, cassandra and dgraph instances
i.e.:

```
# MongoDB
docker run --name mongodb -d -p 27017:27017 mongo

# Cassandra
docker run --name node01 -p 9042:9042 -d cassandra

# Dgraph
docker run --name dgraph -d -p 8080:8080 -p 9080:9080  dgraph/standalone
```

And run the script

```
python main.py
```

### To load data

Menu options:

-   14 (MongoDB)
-   27 (Cassandra)
-   37 (Dgraph)
