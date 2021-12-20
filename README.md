# twitter sentiment analysis
Twitter sentiment analysis<br><br>
TO RUN, YOU NEED TO INCLUDE A PYTHON FILE CALLED twitter_credentials.py THAT DELIVERS PERSONAL AUTHORIZATION CODES.<br><br> 
To run stream: run "spark-submit RUN_twitter_connect.py", then in a separate terminal, run "spark-submit RUN_twitter_stream.py"<br><br>
The results, along with the original tweet, are stored in a Mongo database or "output" folders if Mongo database is not connected.<br>
Sample OUTPUT file of those combined folders is included in this repository.
Training model, and sample of pre-processed input is also included in this repository.
