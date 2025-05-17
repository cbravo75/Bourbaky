def write_file():
    print("Writing a file cbv 20250515..")
    try:
        f = open("my_cbvfile.txt", "a")
        for num in range(100):
            f.write("Line " + str(num) + "\n")
        f.close()
    except Exception:
        print("Could not write to file cbv 20250515")


def read_file():
    print("Now reading the file cbv 20250515..")
    try:
        f = open("info_rep3_noEncontrados.txt", "r")
        for line in f.readlines():
            print(line)
        f.close()
    except Exception:
        print("Could not read to file cbv 20250515")


#write_file()

read_file()