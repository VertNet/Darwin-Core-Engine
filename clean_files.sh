#!/bin/bash

find . -name 'bulkloader-log-*' | xargs rm
find . -name 'bulkloader-progress-*' | xargs rm
find . -name '*.*~' | xargs rm
find . -name '*.pyc' | xargs rm
