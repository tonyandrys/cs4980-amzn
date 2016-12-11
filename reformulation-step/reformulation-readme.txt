CS:4980 - Final Project 
Submission 3 (Reformulate)
Tony Andrys
==========================

The first step in reformulation of amazon review data is to parse and reformat the "categories" list as well as the files containing the product reviews.

*Categories*
The categories list is a one-to-many relation between an Amazon Standard Identification Number (a product's UUID) to the product categories it belongs in. Unfortunately, as orginally downloaded, it arrived in a pretty unhelpful format. It is desirable to re-write this file such that each product (and the categories it relates to) are on one line.

----------------------
Format (as downloaded)
----------------------
A123456             // Product UUID
Books, Self Help
Music, Classical
Music, Classical, CD
Music, Classical, LP
B123456
Film
Film, Horror
Film, Removable Media
...
----------------------

After realizing that a CSV format would be messy (due to the many commas in category names), I wrote the included python script to convert this list into a TSV file, where each line represents one product. It looks something like this:

-------------
Reformulated 
-------------
A123456 Books, Self Help    Music, Classical    Music, Classical, CD    Music, Classical, LP
B123456 Film    Film, Horror    Film, Removable Media,
...
-------------

*Reviews*
The files containing the reviews for each product are not sorted by category (this is not entirely true, because each file is named after a category, and that file contains all the products in that category, but filenames are not a reliable way of sorting). To solve this, I need to reformulate the review data in the same way I reformatted the category data. In other words, going from this:

----------------------
Format (as downloaded)
----------------------

product/productId: B000JVER7W
product/title: Mobile Action MA730 Handset Manager - Bluetooth Data Suite
product/price: unknown
review/userId: A1RXYH9ROBAKEZ
review/profileName: A. Igoe
review/helpfulness: 0/0
review/score: 1.0
review/time: 1233360000
review/summary: Don't buy!
review/text: First of all, the company took my money and sent me an email telling me the product was shipped. A week and a half later I received another email telling me that they are sorry, but they don't actually have any of these items, and if I received an email telling me it has shipped, it was a mistake.When I finally got my money back, I went through another company to buy the product and it won't work with my phone, even though it depicts that it will. I have sent numerous emails to the company - I can't actually find a phone number on their website - and I still have not gotten any kind of response. What kind of customer service is that? No one will help me with this problem. My advice - don't waste your money!

product/productId: B000JVER7W
...
-----------------------

To this:

------------
Reformulated
------------
B000JVER7W  Mobile Action MA730 Handset Manager - Bluetooth Data Suite  unknown A1RXYH9ROBAKEZ  A. Igoe 0/0 1.0 1233360000  Don't buy!  First of all, the company took my money and sent me an email telling me the product was shipped. A week and a half later I received another email telling me that they are sorry, but they don't actually have any of these items, and if I received an email telling me it has shipped, it was a mistake.When I finally got my money back, I went through another company to buy the product and it won't work with my phone, even though it depicts that it will. I have sent numerous emails to the company - I can't actually find a phone number on their website - and I still have not gotten any kind of response. What kind of customer service is that? No one will help me with this problem. My advice - don't waste your money!

B000JVER7W  ...
------------

With both files in a format like this, it should be easy to perform a relational join, in the same way that it was performed in the "IP-Table Join" homework assignment. THEN the data will be *much* easier to manipulate and analyze.
