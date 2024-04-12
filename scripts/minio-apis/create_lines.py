import sys
def main():
    try:
        filename = sys.argv[1]
        num_lines = int(sys.argv[2])
        print(filename)
        file_fd = open(filename, "a")
        line="the quick brown fox jumps over the lazy dog\n"
        lines=""
        for i in range(0,num_lines):
            lines+=line
        file_fd.writelines(lines)
        file_fd.close()
    except:
        print("Command Format is:")
        print("python3 num_lines.py filename number_of_lines(int)")

if __name__=="__main__":
    main()