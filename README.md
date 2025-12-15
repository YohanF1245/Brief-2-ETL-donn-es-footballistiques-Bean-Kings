# Brief-2-ETL-donn-es-footballistiques-Bean-Kings
Second brief data ingénieur p1

## Choix de technologie: 
Au vu de la demande client sur le livrable attendu : 
```
- Une table/collection finale contient exactement les colonnes/champs suivants  sur laquelle vous sortirez quelques KPI !
id_match : identifiant séquentiel (1 = premier match historique ; dernier = finale 2022 France–Argentine)
home_team : équipe 1
away_team : équipe 2
home_result : buts équipe 1
away_result : buts équipe 2
result : vainqueur (home_team / away_team) ou "draw"
date : date du match
round : tour (poules, 8e, 1/4, 1/2, finale, etc.)
city : ville
edition : édition (ex: “1930”, “2014”, “2022”, etc.)
```
Nous remarquons que certaines entités se démarquent (équipes, lieux, dates ...) se qui se prête bien a un modèle entité-relation.