import sys
def main():
    try:
        filename = sys.argv[1]
        path = sys.argv[2]
        num_lines = int(sys.argv[3])
        # print(len(sys.argv))
        if len(sys.argv) == 5:
            # print("hello")
            num_files = int(sys.argv[4])
            
            print(filename)
            filepath = path+filename
            print(filepath)
            file_fd = open(filepath, "w")
            # line="the quick brown fox jumps over the lazy dog\n"
            for num_file in range(0, num_files):
                for i in range(0,num_lines):
                    line="the"+str(i)+" quick"+str(i)+" brown"+str(i)+" fox"+str(i)+" jumps"+str(i)+" over"+str(i)+" the"+str(i)+" lazy"+str(i)+" dog"+str(i)+"\n"
                    file_fd.writelines(line)
            file_fd.close()
        else:
            
            print(filename)
            filepath = path+filename
            print(filepath)
            file_fd = open(filepath, "w")
            # line="the quick brown fox jumps over the lazy dog\n"
            for i in range(0,num_lines):
                line="the"+str(i)+" quick"+str(i)+" brown"+str(i)+" fox"+str(i)+" jumps"+str(i)+" over"+str(i)+" the"+str(i)+" lazy"+str(i)+" dog"+str(i)+"\n"
                file_fd.writelines(line)
            file_fd.close()
    except Exception as error:
        print('An exception occurred: {}'.format(error))

if __name__=="__main__":
    main()