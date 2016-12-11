lineList = []
activeProductId = ""

with open('data/meta/categories.txt') as fp:
    for line in fp:
        # Fired if we are *not* in the middle of adding categories to an ID
        if line[0] + line[1] != "  " and activeProductId == "":
            activeProductId = line.strip("\n")

        # Add categories to lineList
        elif line[0] + line[1] == "  " and activeProductId != "":
            lineList.append(line.strip())

        # New product ID detected - print the current line and change value of activeProductId
        elif line[0] + line[1] != "  " and activeProductId != "":
            # build string of categories for this product delimited by tabs & print
            catStr = "\t".join(lineList)
            print activeProductId + "\t" + catStr

            # set this line (new product id) as active, clear the category array.
            activeProductId = line.strip("\n")
            del lineList[:]
        else:
            raise Exception("FATAL - Something is seriously wrong.")
