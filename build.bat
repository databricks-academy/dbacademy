rmdir /s/q .\dist

C:\Users\jacob.parr\Anaconda3\envs\dbacademy\python.exe setup.py bdist_wheel

rmdir /s/q .\build
rmdir /s/q .\src\dbacademy.egg-info

cd src
pydoc -w dbacademy
call move ./dbacademy.html ../docs/index.html
cd ..