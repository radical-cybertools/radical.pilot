import string
import random

halfMegFile = open('halfMegFile.txt','w')

str = ''

for i in range(524288):
    str += random.choice(string.letters)

halfMegFile.write(str)
halfMegFile.close()

megFile = open('megFile.txt','w')
str += str
megFile.write(str)
megFile.close()

twoMegFile = open('twoMegFile.txt','w')
str += str
twoMegFile.write(str)
twoMegFile.close()
