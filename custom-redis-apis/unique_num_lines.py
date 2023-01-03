import sys
def main():
    try:
        filename = sys.argv[1]
        num_lines = int(sys.argv[2])
        print(filename)
        file_fd = open(filename, "w")
        # line="the quick brown fox jumps over the lazy dog\n"
        for i in range(0,num_lines):
            line="the"+str(i)+" quick"+str(i)+" brown"+str(i)+" fox"+str(i)+" jumps"+str(i)+" over"+str(i)+" the"+str(i)+" lazy"+str(i)+" dog"+str(i)+"\n"
            file_fd.writelines(line)
        file_fd.close()
    except:
        print("Command Format is:")
        print("python3 num_lines.py filename number_of_lines(int)")

if __name__=="__main__":
    main()