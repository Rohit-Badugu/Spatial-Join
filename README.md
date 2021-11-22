# Spatial-Join
Input Data: Input datasets consist of two spatial datasets: a point dataset and a rectangle dataset. The point
dataset (points.csv) consists of latitudes and longitudes of various points while the rectangle dataset
(rectangles.csv) consists of latitudes and longitudes of two diagonal points of rectangles.

Each row in points.csv file has the format longitude,latitude while the same for the rectangles.csv
file is longitude1,latitude1,longitude2,latitude2.

Our goal is to perform spatial join between points dataset and rectangles dataset and return the
number of points inside each rectangle (including points on rectangle boundary). Spatial join is done using PostGIS and Apache Sedona.
