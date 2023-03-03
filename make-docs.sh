#!/bin/sh
rm -rf docs
mkdir docs
cd docs
PYTHONPATH=../src python -m pydoc -w ../src/
cp dbacademy.html index.html

