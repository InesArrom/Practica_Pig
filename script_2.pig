REGISTER /usr/lib/pig/piggybank.jar;

comentaris = LOAD '/user/cloudera/pig_analisis_opinions/critiquescinematografiques.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  AS (text:chararray, label:int, id:int);
pelicules = LOAD '/user/cloudera/pelicules.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  AS (id:int, nom_pelicula:chararray);
comentaris_group = group comentaris by id;

/* Per cada una de les id contam les labels positives i les labels negatives */
countOpinions = foreach comentaris_group
  {
      labels_pos = FILTER comentaris BY label == 1;
      labels_neg = FILTER comentaris BY label == 0;
      labels_total = COUNT(labels_pos)-COUNT(labels_neg);
      GENERATE group as id, COUNT(comentaris.id) as n_comentaris, COUNT(labels_pos) as labels_pos, COUNT(labels_neg) as labels_neg, labels_total as labels_total;
  }

/* Amb la clausula join, ajuntem les pelicules amb el seu n_opinions, les labels positives i les labels negatives */
pelicules_join = join pelicules by id, countOpinions by id using 'replicated';
pelicules_opinions = foreach pelicules_join generate pelicules::id as id, pelicules::nom_pelicula as nom_pelicula, countOpinions::n_comentaris as n_opinions, countOpinions::labels_pos as labels_pos, countOpinions::labels_neg as labels_neg;

STORE pelicules_opinions INTO '/user/cloudera/WorkspacePigAnalisisOpinionsExercici/resultat_analisis_opinions_pelicules' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
