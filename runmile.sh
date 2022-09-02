#!/bin/bash

make startcontainer outputdir=$(pwd)/output testdir=$(pwd)/my_test
make run_mile mile_id=4
