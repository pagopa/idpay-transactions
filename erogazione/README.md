# Script Erogazioni Massive - PagoPA/Invitalia

Questo script Python automatizza l'invio massivo di richieste di erogazione verso l'endpoint `/erogazioni` di Invitalia/PagoPA partendo da un file CSV.

Lo script gestisce automaticamente:
* **Autenticazione OAuth2**: Recupero del Bearer Token tramite chiamata `GET` (Client Credentials Flow).
* **Integrazione SelfCare**: Recupero automatico dei dati anagrafici (CAP, indirizzo, località, provincia, PEC) tramite API SelfCare usando la Partita IVA.
* **Cache Interna**: I dati recuperati da SelfCare vengono memorizzati in cache per evitare chiamate ripetute per la stessa Partita IVA.
* **Mappatura Dati**: Trasformazione dei campi CSV nel payload JSON.
* **Conversione Valori**:
    * *Importo*: da centesimi (input) a Euro (payload).
    * *Date*: da formato italiano con millisecondi a ISO 8601 (`yyyyMMddTHH:mm:ssZ`).
* **Logging**: Salvataggio di tutte le risposte (successi ed errori) in un unico file JSON cumulativo.

## 📋 Prerequisiti

* **Python 3.x** installato.
* Libreria `requests`.

Puoi installare le dipendenze necessarie con il comando:

```bash
pip install requests
```

## ⚙️ Configurazione

Prima di eseguire lo script, apri il file `erogazioni.py` e configura le variabili iniziali con le tue credenziali e URL corretti:

```python
# Credenziali OAuth2 per API Erogazioni
CLIENT_ID = "INSERISCI_QUI_IL_TUO_CLIENT_ID"
CLIENT_SECRET = "INSERISCI_QUI_IL_TUO_CLIENT_SECRET"
BASE_URL = "https://api.invitalia.it/v1"

# Configurazione SelfCare
SELFCARE_URL = "https://api.selfcare.pagopa.it/external/v2/institutions"
SELFCARE_API_KEY = "LA_TUA_SELFCARE_API_KEY"
```

> **⚠️ Attenzione:** Non caricare mai le credenziali reali (`CLIENT_SECRET`, `SELFCARE_API_KEY`) su repository pubblici. Si consiglia di utilizzare variabili d'ambiente o un file `.env` per una maggiore sicurezza.

## 📂 Formato File di Input (`dati.csv`)

Lo script cerca un file chiamato `dati.csv` nella stessa directory di esecuzione.
Il file deve avere le seguenti caratteristiche:
* **Separatore:** Virgola (`,`).
* **Quote:** Virgolette (`"`) per gestire campi contenenti virgole (es. Ragione Sociale).
* **Encoding:** UTF-8.

### Mappatura Colonne

| Intestazione CSV | Descrizione | Note di Elaborazione |
| :--- | :--- | :--- |
| `id` | Identificativo richiesta | Usato come `requestId` nel body e `Request-Id` nell'header. |
| `partitaIvaCliente` | P.IVA Beneficiario | Usato anche per recuperare i dati anagrafici da SelfCare. |
| `codiceFiscaleCliente` | CF Beneficiario | Mappato nel JSON in `anagrafica.codiceFiscaleCliente`. |
| `ragioneSocialeIntestatario` | Ragione Sociale | |
| `ibanBeneficiario` | IBAN accredito | |
| `intestatarioContoCorrente` | Intestatario Conto | |
| `importo` | Importo erogazione | **Inserire in centesimi** (intero).<br>Es: `15000` viene convertito in `150.0`. |
| `autorizzatore` | Nome Autorizzatore | |
| `merchantId` | ID Merchant | Valore non utilizzato dallo script|
| `idPratica` | ID Pratica | Mappato in `erogazione.idPratica`. |
| `dataAmmissione` | Data Ammissione | Formato atteso: `dd/MM/yyyy, HH:mm:ss,fff`.<br>Viene convertito in ISO 8601. |

### Campi recuperati automaticamente da SelfCare

I seguenti campi vengono recuperati automaticamente tramite API SelfCare usando la Partita IVA (`partitaIvaCliente`) come chiave di ricerca:

| Campo Payload | Campo SelfCare | Descrizione |
| :--- | :--- | :--- |
| `anagrafica.cap` | `zipCode` | CAP Sede |
| `anagrafica.indirizzo` | `address` | Indirizzo Sede |
| `anagrafica.localita` | `city` | Località Sede |
| `anagrafica.provincia` | `county` | Provincia Sede |
| `anagrafica.pec` | `digitalAddress` | PEC Beneficiario |

> **⚠️ Nota:** Se la chiamata a SelfCare restituisce un numero di risultati diverso da 1, la riga viene saltata e l'errore viene loggato.

### Esempio CSV Raw
```csv
"id","partitaIvaCliente","codiceFiscaleCliente","ragioneSocialeIntestatario","ibanBeneficiario","intestatarioContoCorrente","importo","autorizzatore","merchantId","idPratica","dataAmmissione"
"6937e6036709039f9db614ba","12345678900","12345678900","alfa S.R.L.","IT43F0000000000000000000000","Alberto","10000","Gianluca Fiorillo","42c37b28-e3b1-37ae-8927-236e6e998a43","alfa-idPratica","09/12/2025, 09:04:00,040"
```

### Generazione del File CSV di Esempio
Per generare il file `dati.csv` è necessario eseguire la seguente query (opt. aggiunta nella where del filtro month == "yyyy-MM"):

```query
merchant
| join kind=inner rewards_batch on $left._id == $right.merchantId
| where status == "APPROVED" and approvedAmountCents > 0
| project id=_id1 ,
    partitaIvaCliente=iff(strlen(vatNumber)==16, "00000000000", vatNumber), 
    codiceFiscaleCliente=fiscalCode, 
// Tagliamo a 140 caratteri per compatibilità con le api di erogazione
    ragioneSocialeIntestatario = substring(businessName, 0, 140),
    ibanBeneficiario=iban, 
    intestatarioContoCorrente=ibanHolder,
    importo=tostring(approvedAmountCents),
    autorizzatore="Gianluca Fiorillo",
    merchantId=_id,
    idPratica=_id1,
    dataAmmissione=updateDate


```

> **📝 Nota:** I campi anagrafici (CAP, indirizzo, località, provincia, PEC) non sono più necessari nel CSV in quanto vengono recuperati automaticamente da SelfCare tramite la Partita IVA.

## 🚀 Utilizzo

1.  Posiziona il file `dati.csv` nella stessa cartella dello script.
2.  Esegui lo script da terminale:

```bash
python erogazioni.py
```

Durante l'esecuzione, lo script mostrerà a video il progresso (ID pratica elaborato e status code).

## 📄 Output (`risposte_api.json`)

Al termine dell'esecuzione, viene generato (o sovrascritto) il file `risposte_api.json`. Questo file contiene l'array di tutti gli esiti.

**Esempio di struttura output:**

```json
[
    {
        "csv_id": "6937ebd16709039f9db6163e",
        "idPratica": "erogazione.idPratica",
        "status_code": 200,
        "timestamp": "2025-12-16T10:00:00.123456",
        "api_response": {
            "timestamp": "2025-12-16T10:00:00Z",
            "codice": 0,
            "idRichiesta": "...",
            "errors": []
        }
    },
    {
        "csv_id": "...",
        "status_code": 400,
        "api_response": "Messaggio di errore testuale o JSON"
    }
]
```

## 🛠 Note Tecniche

* **Gestione Token:** Lo script effettua una chiamata `GET` all'endpoint Microsoft configurato (`https://login.microsoftonline.com/...`) per ottenere il token.
* **Endpoint API Erogazioni:** Le chiamate vengono effettuate in `POST` verso `/erogazioni`.
* **Esito Positivo:** Lo script considera "successo" qualsiasi status code HTTP 2xx.
* **Integrazione SelfCare:**
    * Lo script chiama l'API SelfCare `GET /institutions?taxCode={partitaIvaCliente}` per recuperare i dati anagrafici.
    * Se la risposta contiene un numero di risultati diverso da 1, la riga viene saltata e l'errore viene loggato nel file di output.
    * Una cache interna memorizza i risultati per evitare chiamate duplicate per la stessa Partita IVA.
    * Gli errori delle chiamate SelfCare vengono loggati nel file di output JSON insieme agli errori delle erogazioni.
