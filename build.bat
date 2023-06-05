@echo -----------------------------------------------------------------------------------------------------------------
rmdir /s/q .\dist

@C:\Users\JacobParr\.conda\envs\dbacademy\python.exe setup.py bdist_wheel

rmdir /s/q .\build
rmdir /s/q .\src\dbacademy.egg-info

RMDIR /q docs /Q /S
MKDIR docs
COPY .\docs_src\index.html .\docs\index.html

CD docs
SET PYTHONPATH=..\src
python -m pydoc -w ..\src\

