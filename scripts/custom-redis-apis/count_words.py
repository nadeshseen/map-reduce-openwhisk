import sys
def main():
    # try:
    filename = sys.argv[1]
    # num_lines = int(sys.argv[2])
    print(filename)
    file_fd = open(filename, "r")
    file_contents = file_fd.read()
    # print(file_contents)
    file_contents = file_contents.split()
    print(len(file_contents))
    # except:
    #     print("Command Format is:")
    #     print("python3 num_lines.py filename number_of_lines(int)")

if __name__=="__main__":
    main()