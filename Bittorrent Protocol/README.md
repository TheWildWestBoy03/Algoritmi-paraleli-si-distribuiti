### Pogan Alexandru-Mihail
### Grupa 335CA

# Tema 2 APD - Implementarea protocolului BitTorrent folosind programare distribuita si MPI

### Pentru rezolvarea temei, am pornit de la scheletul dat in fisier, care contine doua threaduri, cate unul pentru download, respectiv upload. De mentionat, totodata, ca am implementat si partea de eficienta a protocolului.

## Peer 


<p style="font-size: 17px"> Pentru inceput, peer-urile citesc hash-urile fisierelor pe care le detin, urmand ca tot acestia sa trimita informatiile legate de fisierul in sine, cat si hash-urile segmentelor din care este compus. </p>

<p style="font-size: 17px"> Acestea asteapta confirmarea trackerului pentru a putea incepe descarcarea fisierelor din retea. </p>

<p style="font-size: 17px"> De asemenea, deblocheaza thread-ul de upload, pentru a nu se crea deadlock-uri, motiv pentru folosirea mutex-ului definit in structura argumentului pasat catre thread-urile din problema.</p>

<p style="font-size: 17px"> Cere de la tracker swarm-ul specific fisierului pe care il doreste.</p>

<p style="font-size: 17px"> Din swarm, ia fiecare client din lista data de la tracker, trimitand cereri pentru segmentele lipsa. Acestea sunt organizate cu ajutorul unui vector de constante, care sa indice daca segmentul a fost sau nu descarcat.</p>

<p style="font-size: 17px"> Clientii asteapta confirmarea pozitiva/negativa de la uploader.</p>

<p style="font-size: 17px"> Peer-urile, ca uploaderi, primesc un tip de request de la peer-uri si tracker. Daca mesajul vine de la peer-uri, verifica daca are segmentul cu hash-ul specificat de clientul doritor. In functie de acest lucru, confirmarea este pozitiva/negativa.</p>

<p style="font-size: 17px"> Dupa ce a descarcat complet un fisier, trimite un request catre tracker, pentru a-s actualiza statutul din PEER, in SEED.</p>

<p style="font-size: 17px"> Daca a descarcat toate fisierele, trimite un anunt catre tracker si isi inchide thread-ul de download.</p>

<p style="font-size: 17px"> Daca in thread-ul de uploader primeste mesajul de finalizare de la tracker, acesta se inchide, iar resursele sunt eliberate ulterior.</p>

## Tracker 

<p style="font-size: 17px"> Intre timp, trackerul primeste informatiile mentionate mai devreme, pastrandu-si o baza de date, cu cate o intrare pentru fiecare client. </p>

<p style="font-size: 17px"> Dupa aceasta, tot trackerul isi construiste swarm-urile initiale, pe care sa le aiba pregatite atunci cand clientii vor sa descarce fisiere.</p>

<p style="font-size: 17px"> Ulterior, trackerul anunta ceilalti clienti ca descarcarea si uploadarea pot incepe. </p>

<p style="font-size: 17px"> Pentru oricare prim mesaj de la client, acesta trimite si hash-urile fisierului dorit si numarul de segmente, pe langa swarm-ul aferent fisierului cerut.</p>

<p style="font-size: 17px"> Pentru orice mesaj de finalizare a descarcarii unui fisier, acesta actualizeaza starea clientului de la PEER, la SEED.</p>

<p style="font-size: 17px"> Pentru orice mesaj de finalizare a descarcarii tuturor fisierelor dorite, acesta isi actualizeaza lista de clienti care au terminat de descarcat.</p>

<p style="font-size: 17px"> Atunci cand toti clientii au terminat de descarcat toate fisierele, acesta trimite mesajul de finalizare catre acestia, isi inchide bucla infisita si isi elibereaza resursele.</p>
 
<p style="font-size: 17px"><b>Eficienta</b> algoritmului este data de faptul ca, trackerul sorteaza potentialii clienti ce au fisierul/bucati de fisier(fara sa cunoasca ce segmente are in realitate fiecare peer) in functie de cate fisiere au acestia, indiferent daca fisierul sortat este seed sau peer. Am ales aceasta abordare, atat datorita simplitatii implementarii, cat si a faptului ca cu cat ai mai multe fisiere detinute(intregi, sau doar bucati din acestea), cu atat esti mai predispus sa primesti cereri de incarcare din partea mai multor clienti. De asemenea, ca metoda de sortare, am folosit counting sort, intrucat in orice scenariu testat in cadrul acestei teme, numarul de fisiere este mic, motiv pentru care alocarea unui vector de frecvente nu reprezinta o problema de gestionare eficienta a memoriei. In plus, din punct de vedere logic, indexarea este pozitiva(nu exista numar negativ de fisiere). </p>