"""
CS:4980:0002 - Big Data Technologies
Tony Andrys 
Final Project - Advanced Version
"""
from pyspark import SparkConf, SparkContext
import numpy
conf = SparkConf()
conf.setMaster("yarn-client")
conf.setAppName("Andrys - Amazon Review (Final Project)")
sc = SparkContext(conf = conf)

"""
Data Structures, Constants, I/O Parameters
"""
S3_DATA_URL_PREFIX = 's3://andrys-cs4980/'      # Path to s3 bucket and directory containing split/gz'd review data.
INPUT_BASE_PATH = '/user/hadoop/input/'         # HDFS - Absolute Path to Input files.
OUTPUT_BASE_PATH = '/user/hadoop/output/'       # HDFS - Absolute Path to directory where output files are to be saved.

# Unique review category identifiers --> REVIEW_URL globs for quick retrieval of review data.
CAT_AMAZON_INSTANT_VIDEO = 0
CAT_AUTOMOTIVE = 1
CAT_BABY = 2
CAT_BEAUTY = 3
CAT_BOOKS = 4
CAT_CELL_PHONES_AND_ACCESSORIES = 5
CAT_CLOTHING_AND_ACCESSORIES = 6
CAT_ELECTRONICS = 7
CAT_GOURMET_FOODS = 8
CAT_HEALTH = 9
CAT_HOME_AND_KITCHEN = 10
CAT_INDUSTRIAL_AND_SCIENTIFIC = 11
CAT_MUSICAL_INSTRUMENTS = 12
CAT_JEWELRY = 13
CAT_KINDLE_STORE = 14
CAT_MOVIES_AND_TV = 15
CAT_MUSIC = 16
CAT_OFFICE_PRODUCTS = 17
CAT_PATIO = 18
CAT_PET_SUPPLIES = 19
CAT_SHOES = 20
CAT_SOFTWARE = 21
CAT_SPORTS_AND_OUTDOORS = 22
CAT_TOOLS_AND_HOME_IMPROVEMENT = 23
CAT_TOYS_AND_GAMES = 24
CAT_VIDEO_GAMES = 25
CAT_WATCHES = 26
CAT_ALL = 27

CATEGORY_TAGS = ["AIV", "AUTO", "BABY", "BEAU", "BOOK", "CELL", "CLOT", "ELEC", "FOOD", "HEAL", "HOME", "INDU", "INST", "JEWE", "KIND", "MOVI", "MUSI", "OFFI", "PATI", "PET", "SHOE", "SOFT", "SPOR", "TOOL", "TOYS", "VIDE", "WATC", "ALL"]

# URLs globs to each category of review data.
REVIEW_URLS = {
        0: S3_DATA_URL_PREFIX + 'Amazon_Instant_Video.tsv-*.gz',
        1: S3_DATA_URL_PREFIX + 'Automotive.tsv-*.gz',
        2: S3_DATA_URL_PREFIX + 'Baby.tsv-*.gz',
        3: S3_DATA_URL_PREFIX + 'Beauty.tsv-*.gz',
        4: S3_DATA_URL_PREFIX + 'Books.tsv-*.gz',
        5: S3_DATA_URL_PREFIX + 'Cell_Phones_&_Accessories.tsv-*.gz',
        6: S3_DATA_URL_PREFIX + 'Clothing_&_Accessories.tsv-*.gz',
        7: S3_DATA_URL_PREFIX + 'Electronics.tsv-*.gz',
        8: S3_DATA_URL_PREFIX + 'Gourmet_Foods.tsv-*.gz',
        9: S3_DATA_URL_PREFIX + 'Health.tsv-*.gz',
        10: S3_DATA_URL_PREFIX + 'Home_&_Kitchen.tsv-*.gz',
        11: S3_DATA_URL_PREFIX + 'Industrial*.gz',
        12: S3_DATA_URL_PREFIX + 'Musical_Instruments.tsv-*.gz',
        13: S3_DATA_URL_PREFIX + 'Jewelry.tsv-*.gz',
        14: S3_DATA_URL_PREFIX + 'Kindle*.gz',
        15: S3_DATA_URL_PREFIX + 'Movies*.gz',
        16: S3_DATA_URL_PREFIX + 'Music.tsv-*.gz',
        17: S3_DATA_URL_PREFIX + 'Office_Products.tsv-*.gz',
        18: S3_DATA_URL_PREFIX + 'Patio.tsv-*.gz',
        19: S3_DATA_URL_PREFIX + 'Pet_Supplies.tsv-*.gz',
        20: S3_DATA_URL_PREFIX + 'Shoes.tsv-*.gz',
        21: S3_DATA_URL_PREFIX + 'Software.tsv-*.gz',
        22: S3_DATA_URL_PREFIX + 'Sports*.gz',
        23: S3_DATA_URL_PREFIX + 'Tools*.gz',
        24: S3_DATA_URL_PREFIX + 'Toys*.gz',
        25: S3_DATA_URL_PREFIX + 'Video_Games*.gz',
        26: S3_DATA_URL_PREFIX + 'Watches.tsv-*.gz',
        27: S3_DATA_URL_PREFIX + '*.gz'
        }

# Review Attribute IDs - used to identify certain elements of a review to be filtered, removed, or joined.
ATTR_PRODUCT_ID = 0
ATTR_PRODUCT_TITLE = 1
ATTR_PRODUCT_PRICE = 2
ATTR_REVIEW_USER_ID = 3
ATTR_REVIEW_PROFILE_NAME = 4
ATTR_REVIEW_HELPFULNESS = 5
ATTR_REVIEW_SCORE = 6
ATTR_REVIEW_DATETIME = 7
ATTR_REVIEW_SUMMARY = 8
ATTR_REVIEW_FULL_TEXT = 9

"""
Utility Functions
- Used for data retrieval, manipulation, filtering, and any other operation that is abstract enough to be used in multiple jobs.
"""

# Retrieves review data from S3 and builds an RDD containing all review data from the desired category. 
# 'category' is an integer from 0-16, each mapping to a specific category in REVIEW_URLS.
# 
#   RDD format:
#   RDD stores one review per line.
#   The attributes of each review are divided into an array of length 10.
#   [productId (ASIN), product title, price, userId, profileName, helpfulness, score, datetime review was published (epoch time), summary (header of review), full text]
def getReviewsByCategory(category):
    url = REVIEW_URLS[int(category)]
    d = sc.textFile(url, use_unicode=False)
    d_split = d.map(lambda l: l.split("\t"))
    return d_split

# Returns an abbreviation of the name of a category (called a file_tag).
# Used in output file names to mark the review category that was used to build the output.
def getTagByCategory(category):
    return CATEGORY_TAGS[int(category)]

# Writes the contents of an RDD to a csv file when given a name for the output directory. 
# Method saves the output inside the directory defined in OUTPUT_BASE_PATH.
# 
# Optional: An array of column names (str) can be passed as columnNames. If defined, these will be written to the first line of the output file.
def writeRDDToCSV(rdd, dirName, columnNames=None):
    csv_data = rdd.map(lambda r: ','.join(map(str, r)))                                 # condense each row element in 'rdd' into one comma delimited string
    if columnNames is not None:                                                         # if column names are provided, build a header and appendto the top of the output rdd
        header = sc.parallelize([','.join(map(str, columnNames))])                  
        out = header.union(csv_data)                                                
        out.saveAsTextFile(OUTPUT_BASE_PATH + dirName)                              
    else:
        csv_data.saveAsTextFile(OUTPUT_BASE_PATH + dirName)                             # write the raw csv data if no column names are provided

# Builds and returns an RDD containing a subset of desired review attributes -- all attribute IDs not in attrList will be thrown out.
#
# Ex:
#   r = getReviewsByCategory(CAT_MUSIC)     // r <- rdd
#   q = reduceToAttributes(r, [ATTR_REVIEW_USER_ID, ATTR_REVIEW_SCORE, ATTR_REVIEW_DATETIME])
#   // each row in q is a tuple s.t. (reviewUserID, reviewScore, reviewDatetime)
def reduceToAttributes(rdd, attrList):
    assert(len(attrList) > 0)                                                           # number of attributes should be a positive non-zero integer no greater than 10, the number of properties in each review.
    assert(len(attrList) <= 10)
    l = [int(x) for x in attrList]                                                      # ensure all attributes are numbers
    d = rdd.map(lambda r: tuple([r[i] for i in l]))                                     # removes row elements that are not in attrList                         
    return d

# Separates a collection of tuple pairs into X and Y lists, feeds lists into numpy, then calculates and returns the slope of the line of best fit.
# tupleList should take the form of: [(X_0, Y_0), (X_1, Y_1), ..., (X_N-1, Y_N-1)]
def leastSquaresSlopeOfXYPairs(tupleList):
    X = [float(e[0]) for e in tupleList]
    Y = [float(e[1]) for e in tupleList]
    l = numpy.polyfit(X,Y,1)
    return l[0]

# Calculates the arithmetic mean of a list of numbers, returns the result.
def calculateMean(data):
    # convert list of strings to list of floats
    f = map(float, data)
    return sum(f)/float(len(f))

# Groups Amazon UserIDs with each of their reviews. Filters all reviews with no associated userID.
# Returns a tuple containing the resulting RDD and the # of reviews after the filtering process --> (outRDD, reviewCount)
#
# 	NOTE: Output RDD's rows take the following form:
# 	(userId, [(dateTime_0, score_0, helpfulness_0), (dateTime_1, score_1, helpfulness_1), ..., (dateTime_N-1, score_N-1, helpfulness_N-1)])
#
def groupReviewPropertiesByUser(rdd):
	a = rdd.filter(lambda r: "unknown" != r[ATTR_REVIEW_USER_ID])
	reviewCount = a.count()																											# Count the number of reviews in the RDD *after* the filtering process
	b = a.map(lambda r: (r[ATTR_REVIEW_USER_ID], (r[ATTR_REVIEW_DATETIME], r[ATTR_REVIEW_SCORE], r[ATTR_REVIEW_HELPFULNESS])))
	c = b.groupByKey()
	d = c.map(lambda (x,y): (x,list(y)))
	return (d, reviewCount)

"""
Jobs
- Non-reusable operations (or job-specific operations) are defined here.
"""

# Reduces all review data in the RDD to their productIDs, time of publish, and score. Groups by product ID
def productMeanLinearRegByScore(rdd, file_tag):
    reducedRdd = reduceToAttributes(rdd, [ATTR_PRODUCT_ID, ATTR_REVIEW_DATETIME, ATTR_REVIEW_SCORE])
    a = reducedRdd.map(lambda (x,y,z): (x,(y,z)))                                       # transform --> (productId, (datetime, score))
    b = a.groupByKey()                                                                  # group review (datetime, score) pairs with like productIDs 
    c = b.map(lambda (x,y): (x,sorted(list(y))))                                        # sort tuples by datetime in ascending order
    d = c.filter(lambda (x,y): len(y) >= 10)                                            # linear regression requires at least ten points to be accurate - throw out all products with fewer than ten reviews.
    e = d.map(lambda (x,y): (y[0][1], leastSquaresSlopeOfXYPairs(y)))                   # find the slope of the trendline for each set of reviews, move first score of each set out to the front.
    f = e.groupByKey()                                                                  # after letting each line's key be the first score the product received, group on this.
    g = f.map(lambda (x,y): (x, list(y)))
    h = g.map(lambda (x,y): (x, calculateMean(y)))                                      # find the average of each set of reviews
    cols = ['Score', 'Slope of Trendline (avg)']
    dir_name = "productMeanLinearRegByScore_" + str(file_tag)
    writeRDDToCSV(h, dir_name, cols)

# Calculates the number of reviews (and percentage of sample) associated with each user, sorts the list in descending order, and outputs the results as a csv.
def calculateNumberOfReviewsPerUser(rdd, file_tag, totalReviewsInSample):
	a = rdd.map(lambda (x,y): (x, len(y)))												# Get number of reviews associated with this user
	b = a.map(lambda (x,y): (y,x))														# Swap (key, value) to sort list by # of written reviews
	c = b.sortByKey(ascending=False)
	d = c.map(lambda (y,x): (x,y))
	e = d.map(lambda (x,y): (x, y, (float(y)/float(totalReviewsInSample))*100))			# Calculate percentage of reviews this user authored (wrt total number of reviews in sample)
	cols = ['UserID','Num. Reviews Authored','Percent']
	dir_name = "numReviewsPerUser_" + str(file_tag)
	writeRDDToCSV(e, dir_name, cols)


# Calculates the number of reviews associated with each product and outputs a frequency distribution of the sample as a csv.
def calculateProductReviewFrequencies(rdd, file_tag):
    c = rdd.map(lambda r: (r[0], r[6]))                                                 # Map each record to a (productId, score) key/value pair. [('B000058A81', '5.0'), ('B000058A81', '5.0'), ...]
    d = c.groupByKey()                                                                  # Group values (review scores) by their productIds (keys)
    e = d.map(lambda r: (r[0], calculateMean(list(r[1])), len(r[1])))                   # Transform --> (productId, scoreMean, reviewCount)
    f = e.map(lambda (x,y,z): (z, [x, y]))                                              # Bring productId to the front of the tuple to act as the key
    g = f.groupByKey()                                                                  # Bring together productIDs with the same number of reviews
    h = g.map(lambda r: (r[0], list(r[1])))
    i = h.map(lambda r: (r[0], len(r[1])))                                              # Transform --> ('reviewCount', # of products with 'reviewCount' reviews)
    j = i.sortByKey(ascending=False)
    cols = ['Num. of Reviews/product', 'Frequency'] 
    dir_name = "productReviewFrequencies_" + str(file_tag)                                        
    writeRDDToCSV(j, dir_name, cols)

# For each possible score (1.0 - 5.0), calculates the mean helpfulness rating over every review in the RDD.
def helpfulnessRatingToScoreByCategory(rdd, file_tag):
    sanitized = rdd.filter(lambda r: "/" in r[5]).filter(lambda r: r[5] != '0/0')         # remove all reviews with malformed helpfulness scores (or none at all)
    a = sanitized.map(lambda r: (r[0], r[5].split("/")[0], r[5].split("/")[1]))           # separate numerator and denominator
    b = a.map(lambda r: (r[0], (float(r[1])/float(r[2]))*100))                            # convert ratio to an integer from [0,100] and group helpfulness rating by the score of the review
    c = b.groupByKey()
    d = c.map(lambda (x,y): (x, list(y)))
    e = d.map(lambda (x,y): (x, calculateMean(y)))                                        # average the helpfulness ratings for each of the five possible review scores (1.0, 2.0, ..., 5.0)
    cols = ['Score', 'Average Helpfulness Rating']
    dir_name = "helpfulnessRatingToScoreInCategory_" + str(file_tag)
    writeRDDToCSV(e, dir_name, cols)

# Groups reviews together by score, then computes and finds the mean word count for each possible score in the RDD.
def calculateLengthScoreByCategory(rdd, file_tag):
    a = rdd.map(lambda r: (r[6], r[9]))                                                   	# (score, full text)
    b = a.map(lambda (x,y): (x, len(y.split(" "))))                                       	# (score, word count)
    c = b.groupByKey()
    d = c.map(lambda (x,y): (x, list(y)))
    e = d.map(lambda (x,y): (x, calculateMean(y)))                                      	# (score, meanWC)
    f = e.sortByKey()
    cols = ['Score', 'Average Words/Review']
    dir_name = "lengthScoreInCategory_" + str(file_tag)
    writeRDDToCSV(f, dir_name, cols)

# Is there a correclation between length of review and helpfulness? 
# ANSWER: A positive correlation, but barely. Much weaker than I thought. 
def calculateLengthHelpfulnessByCategory(rdd, file_tag):
	sanitized = rdd.filter(lambda r: "/" in r[5]).filter(lambda r: r[5] != '0/0')         	# remove all reviews with malformed helpfulness scores (or none at all)
	a = sanitized.map(lambda r: (r[ATTR_REVIEW_HELPFULNESS], r[ATTR_REVIEW_FULL_TEXT]))		# (helpfulness, full text)
	b = a.map(lambda (x,y): (x, len(y.split(" "))))                                       	# (helpfulness, word count)
	c = b.map(lambda (x,y): (x.split("/")[0], x.split("/")[1], y))							# (helpfulness_num, helpfulness_denom, word count)
	d = c.map(lambda (x,y,z): (float(x)/float(y)*100,int(z)))								# (helpfulness score in [0,100], word count)
	e = d.sortByKey(ascending=False)
	f = e.filter(lambda (x,y): x <= 100.0)													# remove weird helpfulness scores that are over 100...
	g = f.groupByKey()
	h = g.map(lambda (x,y): (x, calculateMean(list(y))))									# to reduce the thousands of data points for helpfulness scores of 0 or 100, use only the mean word count for every pair
	i = h.sortByKey(ascending=False)
	cols = ['Helpfulness Score [0-100]', 'Mean Review Word Count']
	dir_name = "helpfulnessRatingToWordCountInCategory_" + str(file_tag)
	writeRDDToCSV(i, dir_name, cols)

def calculateNumProductReviewsWrtPrice(rdd, file_tag):
    a = reduceToAttributes(rdd, [ATTR_PRODUCT_ID, ATTR_PRODUCT_PRICE])                      # remove all but ID and price
    b = a.filter(lambda r: r[1] != "0.00")                                                  # filter all products with 0.00 (these must be errors) or 'unknown' prices
    c = b.filter(lambda r: r[1] != "unknown")                                               
    d = c.groupByKey()
    e = d.map(lambda (x,y): (x,list(y)))                                                 
    f = e.map(lambda (x,y): (y[0], len(y)))                                                 # final transform: (price of product, # of reviews written for this product)
    g = f.sortByKey()
    cols = ['Product Price', '# of Reviews Published']
    dir_name = "numProductReviewsWrtPrice_" + str(file_tag)
    writeRDDToCSV(g, dir_name, cols)

# Processes a category RDD and reduces it to a set of pairs (one per product) containing the price of the product and its average score.
# Output format:
# (15.00, 4.24) // product 1
# (20.00, 5.00) // product 2
# ...
def calculateMeanProductScoreWrtPrice(rdd, file_tag):
    a = reduceToAttributes(rdd, [ATTR_PRODUCT_ID, ATTR_REVIEW_SCORE, ATTR_PRODUCT_PRICE])   
    b = a.filter(lambda r: r[2] != "0.00")                                                  # filter all products with prices of zero, unknown, and remove reviews with "unknown" scores
    c = b.filter(lambda r: r[2] != "unknown")
    d = c.filter(lambda r: r[1] != "unknown")                                               
    e = d.map(lambda (x,y,z): (x, (y,z)))                                                   # (productId, (reviewScore, productPrice))
    f = e.groupByKey()
    g = f.map(lambda (x,y): (x, list(y)))
    h = g.map(lambda (x,y): (y[0][1], y))                                                   # pull price out of the list and let it be the new key
    i = h.map(lambda (x,y): (x, [e[0] for e in y]))                                         # eliminate redundant price elements in y, list(y) now contains all ratings for each product
    j = i.map(lambda (x,y): (x, calculateMean(y)))                                          # Transform --> (price, avgRating)
    k = j.sortByKey()
    cols = ['Product Price', 'Average Review Score']
    dir_name = "meanProductScoreWrtPrice_" + str(file_tag)
    writeRDDToCSV(k, dir_name, cols)


# From an RDD of reviews, calculates the frequencies of each possible review score (1.0, 2.0, 3.0, 4.0, 5.0)
def calculateScoreFrequency(rdd, file_tag):
    # Get number of reviews in this RDD, will be used to calculate percentages
    reviewCount = rdd.count()
    c = rdd.map(lambda r: (r[6], "1"))                                                      # Map each record to a (score, 1) key/value pair.
    d = c.groupByKey()                                                                      # Group like scores together
    e = d.map(lambda r: (r[0], len(list(r[1]))))                                            # Transform --> (score, #_of_occurrences)
    f = e.sortByKey()                                                                       # IMPORTANT: Sorting AFTER adding the percentages triggers a Spark bug! Must do the sorting here.
    g = f.map(lambda r: (r[0], r[1], ((float(r[1])/float(reviewCount))*100)))               # Transform --> (score, #_of_occurrences, % of category)
    # Major operations complete, convert to CSV and write to HDFS
    cols = ['Review Score','Frequency','Percent']
    dir_name = "scoreFrequencyInCategory_" + str(file_tag)
    writeRDDToCSV(g, dir_name, cols)

"""
Main Program Logic
"""

## Perform all operations on the MUSIC RDD, then all reviews in the dataset.
cats = [CAT_MUSIC, CAT_ALL]
for i in cats:

    rdd = getReviewsByCategory(i)
    tag = getTagByCategory(i)

    # map all reviews to a pair s.t (price, meanProductScore) and output to CSV
    calculateMeanProductScoreWrtPrice(rdd, tag)

    # map all reviews to a pair s.t (price, reviewCount) and output to CSV
    calculateNumProductReviewsWrtPrice(rdd, tag)

    # calculate avg slope of trend line by score for all reviews in RDD
    productMeanLinearRegByScore(rdd, tag)

    # calculate product review frequencies
    calculateProductReviewFrequencies(rdd, tag)

    # Calculate score frequency for every review category
    calculateScoreFrequency(rdd, tag)

    # Is there a correlation between word count of review and the score that is given?
    calculateLengthScoreByCategory(rdd, tag)

    # Is there a correlation between length of review and helpfulness?
    calculateLengthHelpfulnessByCategory(rdd, tag)

    # Who writes reviews per category? Who writes reviews on Amazon?
    out = groupReviewPropertiesByUser(rdd)
    calculateNumberOfReviewsPerUser(out[0], tag, out[1])
