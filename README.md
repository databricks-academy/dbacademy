# `DB Academy`

## Purpose
This repository is the main library used in curriculum and other curriculum automations.

## Getting Started
While intended to be used in both notebook/online/remote and "traditional"/offline/local development, this module is specifically structured for local development and as such requires a Conda environment. Instructions to setup the Conda environment are as folllows:
```
conda create -n dbacademy python=3.8
conda activate dbacademy
pip install -r requirements.txt
```
Once setup, unit tests can by ran by executing
```pytest```

## Usage - Notebook/Online/Remote
To use this module in your notebook add to the first command the following call:<br/>
```%pip install git+https://github.com/databricks-academy/dbacademy```

Note: this will reset the Python interpreter removing any local variables or functions declared
before this call.

However, it should be noted that the [Example Course](https://github.com/databricks-academy/example-course-source) defines a more complex/robust methd for importing this library including the ability to use the provided wheel file.
