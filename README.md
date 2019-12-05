# chrono_gtfs
Liaison des données chrono avec les fichiers gtfs. Ce script R recoit en input des fichiers gtfs et des fichiers de données chrono relatives aux passages aux arrêts. En ouput, il crée une table de données  (chrono_gtfs) dans la base de données PostGreSQL indiquée; elle comprend des données enrichies par intégration des fichiers trips, stops, stop_times avec les données de parcours en temps réel du système Chrono. Ce qui permet entre autres fonctionnalités de calculer les écarts entre le planifié et le réel, donc d'obtenir la ponctualité au niveaux de chacun des arrêts du réseau.

Pour rouler ce script:
1) télécharger le script
2) ouvrir le avec un éditeur de texte OU directement dans  le logiciel R
3) Indiquer le chemin des dossiers contenant les fichiers de données gtfs (PATH GTFS) et les fichiers chrono (PATH CHRONO). 
   Aussi bien que les infos de connexion au serveur PSQL (SERVER, PORT, USER, PassWORD, BASE DE DONNÉE).
4) Lancer l'exécution du script. Le temps d'exécution dépend du volume de données. 
   Noter que Le code n'est pas optimisé faute de l'usage boucles au lieu des méthodes apply().
