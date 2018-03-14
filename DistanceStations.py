# -*- coding: utf-8 -*-

import sys
from pyspark import SparkContext
from pyspark.conf import SparkConf
from math import radians, cos, sin, asin, sqrt

# Configuration
conf = SparkConf().setMaster("local[2]").setAppName("My app")
sc = SparkContext(conf=conf)

# Lecture des fichiers d'entrée
lines = sc.textFile(sys.argv[1]) 

# Récupération de la première ligne
header = lines.first()

# Coordonnées du point de départ
lieu=(47.2063698, -1.5643358)
# Récupération de distance max dans le 2e argument
dmax=float(sys.argv[2])


# Calcul de la distance entre deux points, en kilomètres
# Les paramètres d'entrée sont des tuples de (latitude, longitude)
def distance(lieu1, lieu2):
	lat1, lon1, lat2, lon2 = map(radians, [lieu1[0], lieu1[1], lieu2[0], lieu2[1] ])
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a)) 
	km = 6371 * c
	return km
	

# Mappage et filtrage des valeurs :
# 1 On ne prend pas la première ligne du fichier (header)
# 2 Séparation des blocs
# 3 Récupération des blocs : nom, capacité, location
# 4 Formatage des données
# 5 Formatage des données de localication en float
# 6 Filtrage par distance et capacité
# 7 Ajout de la distance en km, avec 3 chiffres après la virgule
# 8 Tri par distance
values=lines.filter(lambda line: line != header)\
.map(lambda lamb: lamb.split("\",\""))\
.map(lambda lamb: (lamb[1], lamb[10], lamb[-1]) )\
.map(lambda (nom, cap, loc): (nom, int(cap), loc[2:len(loc)-2].split(" , ") ) )\
.map(lambda (nom, cap, loc): (nom, cap, (float(loc[0]), float(loc[1]) ) ) )\
.filter(lambda (nom, cap, loc): cap >= 5 and distance(lieu, loc) <= dmax)\
.map(lambda (nom, cap, loc): (round(distance(lieu, loc), 3), (nom, cap, loc) ) )\
.sortByKey()\
.collect()

# Affichage des résultats
print "* * RESULTATS * *"
for val in values:
	print val