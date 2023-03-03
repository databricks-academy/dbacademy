rmdir /s/q .\dist

C:\Users\jacob.parr\Anaconda3\envs\dbacademy\python.exe setup.py bdist_wheel

rmdir /s/q .\build
rmdir /s/q .\src\dbacademy.egg-info

RMDIR /q docs /Q /S
MKDIR docs
CD docs
SET PYTHONPATH=..\src
python -m pydoc -w ..\src\
COPY dbacademy.html index.html

