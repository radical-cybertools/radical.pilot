import string
import random

if __name__ == '__main__':
    
    fileNum = int(sys.argv[1])
    randString = ''

    for i in range(524288):
        randString += random.choice(string.letters)

    for i in range(fileNum):
        halfMegFile = open('halfMeg/File_%4d.txt'%i,'w')
        halfMegFile.write(randString)
        halfMegFile.close()

    randString += randString
    for i in range(fileNum):
        halfMegFile = open('meg/File_%4d.txt'%i,'w')
        megFile.write(randString)
        megFile.close()

    randString += randString
    for i in range(fileNum):
        halfMegFile = open('twoMeg/File_%4d.txt'%i,'w')
        twoMegFile.write(randString)
        twoMegFile.close()
