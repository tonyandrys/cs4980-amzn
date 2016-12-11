outputList = []

fileList = ['Amazon_Instant_Video', 'Automotive', 'Baby', 'Beauty', 'Books', 'Cell_Phones_&_Accessories', 'Clothing_&_Accessories', 'Electronics', 'Gourmet_Foods', 'Health', 'Home_&_Kitchen', 'Industrial_&_Scientific', 'Jewelry', 'Kindle_Store', 'Movies_&_TV', 'Music', 'Musical_Instruments', 'Office_Products', 'Patio', 'Pet_Supplies', 'Shoes', 'Software', 'Sports_&_Outdoors', 'Tools_&_Home_Improvement', 'Toys_&_Games', 'Video_Games', 'Watches']

# ASINs (Amazon Standard Identification Numbers) are alphanumeric blocks with a length of ten characters.
# 'e' is the string to be tested for ASIN-ability
def isAsin(e):
    eSet = set(list(e))
    result = True
    if len(e) != 10:
        result = False
    elif any(c for c in eSet if c.islower()):
        result = False
    return result


for i in range(len(fileList)):    
    lineCount = 0
    with open('data/' + fileList[i] + '.tsv') as fp:
        print "Validating " + fileList[i] + ".tsv..."
        for line in fp:
            lineCount = lineCount + 1
            l = line.split("\t")
            # Alert if less than ten elements exist in this line - it's missing at least one of the review properties
            if len(l) != 10:
                print "ALERT! " + fileList[i] + " does not have ten properties! (line: " + lineCount + ")"
            # If the first element in a row is not an ASIN, alert. 
            if not isAsin(l[0]):
                print "ALERT! " + fileList[i] + " is not an ASIN! (line: " + lineCount + ")"
            # If the "helpfulness" element is not a ratio, alert.
            if not '/' in l[5]:
                print "ALERT! " + fileList[i] + " has an invalid helpfulness value! (line: "+ lineCount + ")"
            # rating scores take the form of 0.0, 0.5, ..., 4.5, 5.0
            if not '.' in l[6]:
                print "ALERT! " + fileList[i] + " has an invalid rating! (line: "+ lineCount + ")"

