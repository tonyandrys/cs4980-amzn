## Tests reformualated category data on four pre-conditions

# ASINs (Amazon Standard Identification Numbers) are alphanumeric blocks with a length of ten characters.
# 'e' should be the string to be tested for ASIN-ability
def isAsin(e):
    eSet = set(list(e))
    result = True
    if len(e) != 10:
        result = False
    elif any(c for c in eSet if c.islower()):
        result = False
    return result


lineCount = 0
with open('catReformed.txt') as fp:
    for line in fp:
        lineCount += 1
        l = line.split("\t")
        print(l)
        # I. No line should have less than two elements
        if len(l) < 2:
            raise Exception("INVALID DATA ON (line " + str(lineCount) + ") - Condition 1 failed!")
        # II. line[0] should always be an ASIN.
        if not isAsin(l[0]):
            raise Exception("INVALID DATA ON (line " + str(lineCount) + ") - Condition 2 failed!")
        # III. line[1] - line[len-1] should never be an ASIN.
        for i in range(1, len(l)):
            if isAsin(l[i]):
                raise Exception("INVALID DATA ON (line " + str(lineCount) + ") - Condition 3 failed!")
        # IV. No element in any line should be identical.
        if len(l) != len(set(l)):
            print "l = " + str(l)
            print "s = " + str(set(l))
            raise Exception("INVALID DATA ON (line " + str(lineCount) + ") - Condition 4 failed!")
    print "Everything is OK!" 
        
        

