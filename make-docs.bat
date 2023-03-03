@ECHO OFF
RMDIR /q docs /Q /S
MKDIR docs
CD docs
SET PYTHONPATH=..\src
python -m pydoc -w ..\src\
COPY dbacademy.html index.html

