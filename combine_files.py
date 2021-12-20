""" Execution script"""

from filehandling import Filehandling
import os


if __name__ == '__main__':

    start_directory_number = 1640035972000
    endin_directory_number = 1640036030000

    original = Filehandling.current_directory_path()
    fileobject = Filehandling.open_file("OUTPUT-COMBINED.txt")

    for iterate in range(start_directory_number, endin_directory_number + 2000, 2000):
        filepath = Filehandling.output_directory_path(iterate)
        print(filepath)
        os.chdir(filepath)

        for file in os.listdir(filepath):
            print(file)
            try:
                with open(file, 'r') as newfile:
                    content = newfile.read()
                    print(newfile.read())
                    Filehandling.write_to_file(fileobject, content + '\n')
                    Filehandling.close_file(newfile)
            except:
                print("NOPE")
                pass

        os.chdir(original)

    fileobject = Filehandling.close_file(fileobject)
    print("That's all, Folks!")
