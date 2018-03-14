# Recherche de la station la plus proche

## But

Trouver la station de parking de vélo la plus proche de coordonnées données. 

Nous partons d’un fichier de données OpenData fourni par la métropole Nantaise : l’emplacement des stations de vélo ainsi que leur capacité.

## Fonctionnement

Le code est écrit en Python, on utilise PySpark afin de profiter des fonction de mapping et filtrage de Spark. On utilise PySpark en mode standalone.

## Utilisation

Spark et Python doivent être installés au préalable, et le path configuré afin de pouvoir lancer la commande spark-submit pour soumettre notre code.

La commande prend deux arguments : 
* le fichier csv d'entrée
* la distance maxi en kilomètres

Exemple :  
`spark-submit DistanceStations.py resources 1`