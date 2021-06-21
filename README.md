# Exam project - Big Data - Chess Games
## Poggi Giovanni
### Big Data course (81932), University of Bologna.

This repository must be used as a template to create and deliver the exam project. It contains:

- The basic structure for MapReduce and Spark jobs.
- Guidelines for complex MapReduce jobs.
- The template for the report.

Project delivery consists in sending to the teachers the link to this individual assignment.

## Istruzioni per eseguire il Job di MapReduce

Prima di eseguire il seguente comando, accedere al Cluster Unibo con un Tunnel in autenticazione attraverso isi-alfa.csr.unibo.it.
Successivamente accedere al Cluster nel nodo 9 e spostarsi nella cartella /home/gpoggi/. A quel punto lanciare il seguente comando:

> hadoop jar BDE-mr-surname1-surname2.jar multipleJobs.ChessDriver chessanalisys/ stageout/ outputchess/ finaloutput/

I risultati ottenuti si potranno osservare tramite Hue Browser accedendo alla cartella gpoggi/outputchess/

N.B. In caso si acceda da un account che non potrà accedere alla cartella /home/gpoggi, è necessario effettuare dei passaggi preliminari per poter eseguire il programma. Come prima cosa posizionarsi nella propria cartella /home/ seguita dal Nome dell'utente loggato, creare le seguenti directory (chessanalisys, finaloutput, outputchess, stageout) ed inserire il file .CSV relativo alle partite di Scacchi dentro la cartella chessanalisys (Link nella Relazione). Successivamente caricare tramite WinSCP il file .JAR ed a quel punto si potrà eseguire il comando sopra indicato. Per quanto riguarda l'account exams2021, ho già provveduto ad effettuare queste operazioni preliminari. 

## Istruzioni per eseguire il Job di Spark

Prima di eseguire il seguente comando, accedere al Cluster Unibo con un Tunnel in autenticazione attraverso isi-alfa.csr.unibo.it.
Successivamente accedere al Cluster nel nodo 9 e spostarsi nella cartella /home/gpoggi/. A quel punto lanciare il seguente comando:

> spark2-submit --class ChessAnalysisSparkJobs BDE-spark-poggi.jar chessanalisys/chess_games.csv sparkstageout/chess_games_output.csv sparkoutputchess/chess_games_output_final.csv

I risultati ottenuti si potranno osservare tramite Hue Browser accedendo alla cartella gpoggi/sparkoutputchess/

N.B. In caso si acceda da un account che non potrà accedere alla cartella /home/gpoggi, è necessario effettuare dei passaggi preliminari per poter eseguire il programma. Come prima cosa posizionarsi nella propria cartella /home/ seguita dal Nome dell'utente loggato, creare le seguenti directory (chessanalisys, sparkoutputchess, sparkstageout) ed inserire il file .CSV relativo alle partite di Scacchi dentro la cartella chessanalisys (Link nella Relazione). Successivamente caricare tramite WinSCP il file .JAR ed a quel punto si potrà eseguire il comando sopra indicato. Per quanto riguarda l'account exams2021, ho già provveduto ad effettuare queste operazioni preliminari.
