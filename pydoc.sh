#!/bin/sh
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT
cd src
open -a "Google Chrome" "http://localhost:50001/dbacademy.html"
python -m pydoc -p 50001

