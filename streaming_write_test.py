# create a test stream of data from csv file
from random import randint
import time

""" Creates 8 new csv files, one by one in 4 seconds intervals. """

def main():

    run_number = 12
    interval_time = 4
    a = 1

    with open('processed_test_tweets.csv', 'r') as file:
        # read content from csv file
        lines = file.readlines()

        while a <= run_number:
            linecount = len(lines)
            linenumber = randint(0, linecount - 10)
            with open('output{}.csv'.format(a), 'w') as writefile:
                writefile.write(' '.join(line for line in lines[linenumber:linenumber + 25]))
            print('Created new output file output{}.csv'.format(a))
            a += 1
            time.sleep(interval_time)

    print("That's all, Folks!")

if __name__ == '__main__':
    main()
