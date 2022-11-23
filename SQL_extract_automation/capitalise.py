with open("C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\columns to capitalise.txt") as f:
    data = f.readlines()


output = []
for i in data:
    output.append(i.capitalize())


file = open("C:\\Users\\samukhia\\OneDrive - Capgemini\\Desktop\\Work\\Home Office\\capitalised columns.txt", "w")
for i in output:
    file.write(i)

file.close()