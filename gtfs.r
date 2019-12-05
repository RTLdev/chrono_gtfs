
#################################################################################################################
#   ETL-GTFS2PSQL version 1.0.0 Novembre 2019                                                                   #
#   Ce programme est un script ETL écrit en R par Kelogue Thérasmé, Réseau de transport de Longueuil.           #
#   Le script va lire le dossier de fichiers GTFS (chemin) et extraire les fichiers trips, stops et stop_times. #
#   Les fichiers cibles doivent être au format csv. Chaque fichier est lu et les donnÃ©es sont poussées         # 
#   vers trois tables distinctes dans la Base de données PostGreSQL>>>Achaldep (stops_chrono, trips_chrono      #
#   et stop_times_chrono). Plusieurs champs ou clés sont ajoutés par manipulation et jointure de DF en vue de   #
#   lier les tables GTFS avec les données de Chrono (voir table passages_chronos et l'ETL-CHRONO2PSQL)          #                                                       #
#################################################################################################################

rm(list=ls())

if (!require(tidyverse)) install.packages("tidyverse")
if (!require(devtools)) install.packages("devtools")
if (!require(dplyr)) install.packages("dplyr")
if (!require(descr)) install.packages("descr")
if (!require(xtable)) install.packages("xtable")
if (!require(pastecs)) install.packages("pastecs")
if (!require(readr)) install.packages("readr")
if (!require(RPostgreSQL)) install.packages("RPostgreSQL")

library(needs)
needs(readr,tidyverse, RPostgreSQL, devtools, dplyr, descr, xtable, pastecs, readr, tidyverse)

##Lire le repertoire
chemin <- "PATH GTFS"
if(dir.exists(chemin)) listfiles<- unique(list.files(path=chemin, pattern="*.csv", full.names = F))
nbFiles <- length(listfiles)

## Liste de fichiers déjà traités
listeFichiers_dejaTraites<-''
if(file.exists(paste(chemin,'fichierstraites.txt', sep=""))){
  listeFichiers_dejaTraites<- read.delim(paste(chemin,'fichierstraites.txt',  sep=""),header=F, sep="\t", stringsAsFactors =F )
  listeFichiers_dejaTraites$V2<- str_split(listeFichiers_dejaTraites$V1, ' ', 3)[[1]][2]
  listeFichiers_dejaTraites$V3<- str_split(listeFichiers_dejaTraites$V1, ' ', 3)[[1]][3]
  listeFichiers_dejaTraites$V1<- str_split(listeFichiers_dejaTraites$V1, ' ', 3)[[1]][1]
  
  ## Liste de fichiers à traiter
  listfiles2<- list()
  i<-1
  while(i<length(listfiles)) {
    if(!listfiles[i] %in% listeFichiers_dejaTraites$V1){
      listfiles2<- c(listfiles2, listfiles[i])
    }
    i<-i+1
  }
} else { 
  listfiles2<- listfiles 
}
aTraiter<- length(listfiles2)

## Liste des assignations en traitement
listeassignation <- list()

## Démarrer le traitement de chaque fichier GTFS
for(f in 1:length(listfiles2)){
  ## Quelle assignation
  assignation<- substr(listfiles2[f], 3,10) ##hypothèse que le nom du fichier est PAaaaammjj........csv
  listeassignation<- c(listeassignation, assignation)
}

listeassignation<-unique(listeassignation)
len<- length(listeassignation)

## Fichier log (resume.txt)
logfil <- paste(chemin, 'resume.txt', sep='')
text1 <- paste('nombre de fichiers csv dans le répertoire GTFS: ', nbFiles, sep=' ' )
text2 <- paste('Déjà traités : ', nbFiles-aTraiter, sep=' ')
text3 <- paste('à traiter : ', aTraiter, sep=' ')
text4 <- 'Assignation(s) concernée(s) : '
text5 <- paste( '  >>> ', as.character(listeassignation), sep=' ')

write.table("========================================================" , logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(Sys.time() , logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table("========================================================" , logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text1 , logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text3 , logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text4 , logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text5 , logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")

for(fi in 2:len){
  assignation<-listeassignation[[fi]]
  print(paste('Assignation :', assignation))
  assi<- paste(substr(assignation, 1,4),substr(assignation, 5,6),substr(assignation, 7,8), sep='-') ## aaaa-mm-jj
  
  ##Charger trips
  trips_xsl<-read.csv(paste(chemin, 'PA', assignation,'_trips.csv', sep=''), stringsAsFactors = FALSE)
  head(trips_xsl)
  
  ##Rename la colonne PA as pa
  colnames(trips_xsl)[1]<-'assignation'
  
  # ajouter voiture et cle_trips , puis les remplir
  trips_xsl$voiture<- ''
  trips_xsl$cle_trips<- ''
  
  ## Remplir ou modifier les colonnes assignation, voiture et cle_trips
  for(i in 1:nrow(trips_xsl)) {
    trips_xsl$assignation[i]<- assi
    trips_xsl$voiture[i]<- str_split(trips_xsl$block_id[i], '_', 2)[[1]][1]
    
    ##corriger trip_short_name pour y ajouter _service_id si jour Férié (besoin de lier avec chrono)
    if(trips_xsl$service_id[i] %in% c('SE', 'SA', 'DI')) tsname<- trips_xsl$trip_short_name[i] 
    else tsname<- paste(trips_xsl$trip_short_name[i], '_', trips_xsl$service_id[i], sep='')
    trips_xsl$cle_trips[i]<- paste(assignation, '_',trips_xsl$route_id[i],'_', trips_xsl$service_id[i], '_', trips_xsl$voiture[i],'_',tsname, sep="")
  }
  
  ## Logger
  write.table(paste(Sys.time(), ':  PA', assignation,'_trips.csv est lu', sep=''), logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
  
  ##Charger stops##
  stops_xsl<-read.csv(paste(chemin, 'PA', assignation,'_stops.csv', sep=''), colClasses=c("departure_time"="character"), stringsAsFactors = FALSE)
  
  ##Rename la colonne PA as pa
  colnames(stops_xsl)[1]<-'assignation'
  stops_xsl$assignation <-assi
  head(stops_xsl, n=10)
  
  ## Logger
  write.table(paste(Sys.time(), ':  PA', assignation,'_stops.csv est lu', sep=''), logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
 
  ##Charger stop_times ##
  stop_times_xsl<-read.csv(paste(chemin, 'PA', assignation,'_stop_times.csv', sep=''))
  
  colnames(stop_times_xsl)[1]<-'assignation'
  stop_times_xsl$assignation<-assi
  
  ## Logger
  write.table(paste(Sys.time(), ':  PA', assignation,'_stop_times.csv est lu', sep=''), logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")

  ## ajouter timepoint3
  stop_times_xsl$timepoint3<- stop_times_xsl$timepoint
  
  ## liste des courses planifiées (trip_id)
  ncourses <- unique(stop_times_xsl$trip_id)
  
  minValue <- function (df){
    return (min(df$shape_dist_traveled))
  }
  
  maxValue <- function (df){
    return (max(df$shape_dist_traveled))
  }
  n <- length(ncourses)
  zt<- 1
  while(zt<n){
    mini<- stop_times_xsl %>% filter(trip_id==ncourses[zt]) %>% minValue
    maxi<- stop_times_xsl %>% filter(trip_id==ncourses[zt]) %>% maxValue
    stop_times_xsl[ which(stop_times_xsl$trip_id==ncourses[zt] & stop_times_xsl$shape_dist_traveled == mini), 13] <-'D'
    stop_times_xsl[ which(stop_times_xsl$trip_id==ncourses[zt] & stop_times_xsl$shape_dist_traveled == maxi) , 13] <-'F'
    zt<- zt+1
  }
  
  head(stop_times_xsl)
  
  ### TEST SUR DIFFERENCE DE CALCUL DE TIMEPOINT
  test1<- data.frame() 
  i=1
  while (i<nrow(stop_times_xsl)){ 
      if(stop_times_xsl$timepoint3[i]!=stop_times_xsl$timepoint2[i]) { 
        test1<- rbind(test1, stop_times_xsl[i-1,])
        test1<- rbind(test1, stop_times_xsl[i,])
        test1<- rbind(test1, stop_times_xsl[i+1,])
        test1<- rbind(test1, stop_times_xsl[i+2,])
     } 
     i=i+1
  }
  nrow(test1)
  
  ## Fusionner Stop et stop_times avec merge() de R pour y ajouter stop_code (pour lier avec chronos)
  ## extraire stop code de stop_xsl
  stop_code<- data.frame(stops_xsl$stop_code, stops_xsl$stop_id)
  colnames(stop_code) <- c("stop_code","stop_id")    
 
  ## fusion
  stop_times_xsl<- merge(stop_times_xsl, stop_code, by.x='stop_id', by.y='stop_id', all.x = TRUE)
  str(stop_times_xsl)
  
  ## ajouter cle_stop_times
  stop_times_xsl$cle_stop_times<-''
  for(i in 1:nrow(stop_times_xsl)) {
    stop_times_xsl$cle_stop_times[i]<- paste(assignation, stop_times_xsl$trip_id[i], stop_times_xsl$stop_code[i], as.character(stop_times_xsl$departure_time[i]), sep='_' ) 
  }
  
  ## Logger
  write.table(paste(Sys.time(), ':  timepoint, stop_code, Clé stop_times sont ajoutés dans stop_times', sep=''), logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
  
  ## Se connecter au server psql BDD
  drv <- dbDriver("PostgreSQL")
  conn <-dbConnect(drv, host='IP OR ADDRESS OR LOCALHOST', port='NUMBER',dbname='BDD NAME',user='username', password='PASSWRD')
  
  ## Logger
  write.table(paste(Sys.time(), ':  Connection au serveur (PSQL) de données réussie', sep=''), logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
  
  ## POUSSER TRIPS et STOPS_TIMES DANS BDD
  
  ## Pousser les données PSQL tables de destination les données extraites
  ## Respecter l'ordre des champs/doit être même dans DF et PSQL
  dbWriteTable(conn, c("public","trips_chrono"), value=trips_xsl,append=TRUE, row.names=FALSE)
  write.table(paste(Sys.time(), ':  PA', assignation,'_trips.csv (',nrow(trips_xsl),' lignes) est transféré vers la table trips_chrono', sep=''), logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
 
  dbWriteTable(conn, c("public","stops_chrono"), value=stops_xsl,append=TRUE, row.names=FALSE)
  write.table(paste(Sys.time(), ':  PA', assignation,'_stops.csv (',nrow(stops_xsl),' lignes) est transféré vers la table trips_chrono', sep=''), logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
  
  dbWriteTable(conn, c("public","stop_times_chrono"), value=stop_times_xsl,append=TRUE, row.names=FALSE)
  write.table(paste(Sys.time(), ':  PA', assignation,'_stop_times.csv (',nrow(stop_times_xsl),' lignes) est transféré versla table trips_chrono', sep=''), logfil, append =T, row.names=F, col.names=F, quote=F, sep="\t")
  
  ## Enregistrer la liste des fichiers traités
  write.table(paste( paste('PA', assignation,'_stops.csv ', sep=''),  as.character(Sys.time()), sep=""), 'Q:/SAEIV/1-Analyse/2-Comparatif_donnees/4_Comparatif_201811_a_201905/2-Doc_Travail/GTFS/fichierstraites.txt', append =T, row.names=F, col.names=F, quote=F, sep="\t")
  write.table(paste( paste('PA', assignation,'_trips.csv ', sep=''),  as.character(Sys.time()), sep=""),  'Q:/SAEIV/1-Analyse/2-Comparatif_donnees/4_Comparatif_201811_a_201905/2-Doc_Travail/GTFS/fichierstraites.txt', append =T, row.names=F, col.names=F, quote=F, sep="\t")
  write.table(paste( paste('PA', assignation,'_stop_times.csv ', sep=''),  as.character(Sys.time()), sep=""), 'Q:/SAEIV/1-Analyse/2-Comparatif_donnees/4_Comparatif_201811_a_201905/2-Doc_Travail/GTFS/fichierstraites.txt', append =T, row.names=F, col.names=F, quote=F, sep="\t")
}
5+5

rm(list=ls())

#################################################################################################################
#   ETL-CHRONO2PSQL version 1.0.0 Novembre 2019                                                                 #
#   Ce programme est un script ETL écrit en R par Kelogue Thérasmé, RÃ©seau de transport de Longueuil.          #
#   Le script va lire le dossier de fichiers cHRONO (chemin). Les fichiers cibles doivent être au format csv.   #
#   Chaque fichier est lu et les données sont poussées vers la table passages_chronos dans la Base de données   #   
#   PostGreSQL>>>Achaldep. Plusieurs colonnes de données y sont ajoutées par transformation et jointure avec    #
#   les tables GTFS (voir table trips_chrono, stops_chrono et stop_times_chronos de la mÃªme BDD et aussi le    #
#   script ETL associé ETL-GTFS2PSQL.                                                                           #
#################################################################################################################

if (!require(needs)) install.packages("needs")
if (!require(tidyverse)) install.packages("tidyverse")
if (!require(RPostgreSQL)) install.packages("RPostgreSQL")
library(needs)
needs(readr,tidyverse, RPostgreSQL)

## Lire le repertoire
chronos<- 'PATH CHRONO'
if(dir.exists(chronos)) listfiles<- list.files(path=chronos, pattern="*.csv", full.names = F)
nbFiles <- length(listfiles)

## Liste de fichiers déjà traités
if(file.exists(paste(chronos,'fichierstraites.txt',  sep=""))){
  listeFichiers_dejaTraites<- read.delim(paste(chronos,'fichierstraites.txt',  sep=""),header=F, sep="\t", stringsAsFactors =F )
  listeFichiers_dejaTraites$V2<- str_split(listeFichiers_dejaTraites$V1, ' ', 3)[[1]][2]
  listeFichiers_dejaTraites$V3<- str_split(listeFichiers_dejaTraites$V1, ' ', 3)[[1]][3]
  listeFichiers_dejaTraites$V1<- str_split(listeFichiers_dejaTraites$V1, ' ', 3)[[1]][1]
  
  ## Liste de fichiers à traiter
  listfiles2<- list()
  i<-1
  while(i<length(listfiles)) {
    if(!listfiles[i] %in% listeFichiers_dejaTraites$V1){
      listfiles2<- c(listfiles2, listfiles[i])
    }
    i<-i+1
  }
} else { 
  listfiles2<- listfiles 
}

aTraiter<- length(listfiles2)

## Liste des assignations en traitement
listeassignation <- list()

## Démarrer le traitement de chaque fichier GTFS
for(f in 1:len){
  ## Quelle assignation
  assignation<- substr(listfiles2[f], 1,6) ##hypothèse que le nom du fichier est PAaaaammjj........csv
  assi<- paste(substr(assignation, 1,4),substr(assignation, 5,6),substr(assignation, 7,8), sep='-')
  listeassignation<- c(listeassignation, assignation)
}
listeassignation<- unique(listeassignation)

## Fichier log (resume.txt)
logfile <- paste(chronos, 'resume.txt', sep='')
text1<- paste('nombre de fichiers csv dans le répertoire GTFS: ', nbFiles, sep=' ' )
text2<- paste('Déjà traités : ', len-aTraiter, sep=' ')
text3<- paste('à traiter : ', aTraiter, sep=' ')
text4<- 'Assignation(s) concernée(s) : '
text5<- paste( '  >>> ', as.character(listeassignation), sep=' ')

write.table("========================================================" , logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(Sys.time() , logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table("========================================================" , logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text1 , logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text2 , logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text3 , logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text4 , logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
write.table(text5 , logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")

#Pour connecter au psql BDD
drv <- dbDriver("PostgreSQL")
conn <-dbConnect(drv, host='IP OR ADDRESS OR LOCALHOST', port='NUMBER',dbname='BDD NAME',user='username', password='PASSWRD')
write.table( paste(Sys.time() , ' : connexion au serveur de données PostGreSQL>>> OK.', sep=''), logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")

## Lire les fichiers 1 à 1
i=1
while(i<=aTraiter){
  df1<- read.csv(file=paste(chronos,listfiles2[i], sep=''), header=TRUE, sep=";")
  nrow(df1)
  
  ## Renommer les colonnes
  colnames(df1)<- c('date', 'ligne', 'voyage', 'voiture', 'chauffeur', 'vehicule', 'code_arret', 'nom_arret', 'heurereel', 'heureplan', 'adherencedepart', 'montants', 'descendants')
  str(df1)
  df1$date<- as.Date(df1$date)
  df1$voyage<- as.character(df1$voyage)
  df1$nom_arret<- as.character(df1$nom_arret)
  write.table( paste(Sys.time() , ' : le fichier ', listfiles2[i], ' est lu (', nrow(df1), ' lignes extraites)', sep=''), logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
  
  ## POUSSER la table passages_chrono DANS BDD
  dbWriteTable(conn, c("public","passages_chronos"), value=df1,append=TRUE, row.names=FALSE)
  print( paste('Fichier: ', i))
  write.table( paste(i, ') ' , Sys.time() , ' : le fichier ', listfiles2[i], ' est traitÃ© et transfÃ©rÃ© ', sep=''), logfile, append =T, row.names=F, col.names=F, quote=F, sep="\t")
  i=i+1
}

## Ajouter les Assignation, type_service, cle_chronos dans la table passages_chronos 
reqq<- "alter table if exists passages_chronos ADD column if not exists assignation Date, ADD column if not exists type_service varchar(3), ADD column if not exists cle_voyage_chrono varchar(250);"
dbSendQuery(conn, reqq) 

## Mettre a jour ces deux colonnes par jointure avec calendriers et assignations, puis finalement la colonne cle
reqq_assi<-"update passages_chronos set assignation = cast(assignations.assignation as Date) from assignations where passages_chronos.date between cast(assignations.assignation as Date) and cast(assignations.date_fin as Date);"
dbSendQuery(conn, reqq_assi)

## Ajouter les types de service
reqq_type<-"update passages_chronos set type_service='SE' where extract(dow from date) between 1 and 5; update passages_chronos set type_service='SA' where extract(dow from date)=6; update passages_chronos set type_service='DI' where extract(dow from date)=0; " 
dbSendQuery(conn, reqq_type)

## Ajouter les types de services fériés
reqq_ferie<-"update passages_chronos set type_service=calendriers.type_service from calendriers where passages_chronos.date=calendriers.date_ferie; "
dbSendQuery(conn, reqq_ferie)

## Ajouter la cle_voyage_chrono
cle_voyage_chrono<-"update passages_chronos set cle_voyage_chrono=concat(substring( concat('',assignation), 1,4),substring(concat('',assignation), 6,2),substring(concat('',assignation), 9,2), '_',ligne,  '_',type_service, '_', voiture,  '_',voyage);"
dbSendQuery(conn, cle_voyage_chrono)

## Lier passages_chronos et trips_chrono
reqq_UpdatetripId<- " select a.*, b.trip_id into trips_chronos from passages_chronos as a left join trips_chrono as b On a.cle_voyage_chrono = b.cle_trips;"
dbSendQuery(conn, reqq_UpdatetripId)

reqq_UpdateDirection<- "alter table trips_chronos add column if not exists direction char(3);update trips_chronos set direction=split_part(trip_id, '_', 3)  ;"
dbSendQuery(conn, reqq_UpdateDirection)

## Ajouter la cle arret chrono
reqq_cle_arret<- "alter table trips_chronos add column if not exists cle_arretchrono varchar(100);"
dbSendQuery(conn, reqq_cle_arret)

cle_arret_chrono<-"update trips_chronos set cle_arretchrono=concat(substring(concat('',assignation), 1,4),substring(concat('',assignation), 6,2),substring(concat('',assignation), 9,2), '_',trip_id,  '_',code_arret, '_', heureplan);"
dbSendQuery(conn, cle_arret_chrono)

##Lier trips_chronos et stop_times et pousser vers table finale
reqq_chrono_gtfs<-"select a.assignation, a.date, a.type_service, a.ligne, a.direction, a.voyage, b.trip_id, a.voiture, a.chauffeur, a.vehicule, a.code_arret, a.nom_arret, a.heurereel, a.heureplan, a.adherencedepart,b.timepoint3 as timepoint, b.cle_stop_times into chrono_gtfs from stop_times_chrono as b left join  trips_chronos as a ON b.cle_stop_times=a.cle_arretchrono where a.ligne <500 OR a.ligne>700;"
dbSendQuery(conn, reqq_chrono_gtfs)

5+5
