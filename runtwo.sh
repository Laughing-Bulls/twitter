#!/bin/bash
spark-submit streaming_read_test.py &
python streaming_write_test.py &
