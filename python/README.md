### install python 3.10 in WSL

1. `sudo apt install software-properties-common`
2. `sudo add-apt-repository ppa:deadsnakes/ppa`
3. `sudo apt install python3.10`

### install pip3, venv

1. `sudo apt install python3-pip`
2. `sudo apt install python3.10-venv`

### create venv

1. `python3.10 -m venv py310`

### install pyspark in venv

1. `source py310/bin/activate`
2. `pip install pyspark`