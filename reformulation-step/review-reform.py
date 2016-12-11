outputList = []

# Filenames to process (without relative path and file extension)
fileList = ['Amazon_Instant_Video', 'Automotive', 'Baby', 'Beauty', 'Books', 'Cell_Phones_&_Accessories', 'Clothing_&_Accessories', 'Electronics', 'Gourmet_Foods', 'Health', 'Home_&_Kitchen', 'Industrial_&_Scientific', 'Jewelry', 'Kindle_Store', 'Movies_&_TV', 'Music', 'Musical_Instruments', 'Office_Products', 'Patio', 'Pet_Supplies', 'Shoes', 'Software', 'Sports_&_Outdoors', 'Tools_&_Home_Improvement', 'Toys_&_Games', 'Video_Games', 'Watches']

for i in range(len(fileList)):
    
    # Open file to write
    # Filename formatting: category.txt is read --> category.tsv (reformulated) is written
    writef = open('data/' + fileList[i] + '.tsv', 'w')
    print '** Opened data/' + fileList[i] + '.tsv' + ' for writing...'
    
    # Open file to read and iterate by line
    lineCount = 0
    with open('data/' + fileList[i] + '.txt') as fp:
        for line in fp:
            lineCount += 1
            # A line with at least one colon is guaranteed to be a review property
            if ":" in line:
                l = line.split(":")
                
                # l[0] is key, l[1] - l[len(l)-1] is the value (possibly split if colon exists in value)
                # strip leading/trailing whitespace from all elements of l, and newline character from l[len-1] 
                assert(len(l) > 1)
                l = map(lambda e: e.strip(), l) 
                l[len(l)-1] = l[len(l)-1].strip("\n")
                
                # if length of valueList > 2, we must regroup the value back into one piece (joining on the colon that existed before the split)
                value = ""
                if len(l) > 2:
                    value = ": ".join(l[1:])
                else:
                    value = l[1]
                outputList.append(value)
            else:
                # a blank line separates reviews - when one is read, all properties of the review we are processing has been read.
                # write the properties for the review we just finished reading and empty the property list.
                catStr = "\t".join(outputList)
                writef.write(catStr + "\n")
                del outputList[:]
                # print status for fun
                if lineCount % 30000 == 0:
                    print fileList[i] + ".txt -- wrote 30,000 lines to .tsv"
    
    # txt file has been completely read and tsv file can be closed. Loop back to start the next file.
    writef.close()
    print '** Done! **'
