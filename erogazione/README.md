# Script Erogazioni Massive - PagoPA/Invitalia

Questo script Python automatizza l'invio massivo di richieste di erogazione verso l'endpoint `/erogazioni` di Invitalia/PagoPA partendo da un file CSV.

Lo script gestisce automaticamente:
* **Autenticazione OAuth2**: Recupero del Bearer Token tramite chiamata `GET` (Client Credentials Flow).
* **Mappatura Dati**: Trasformazione dei campi CSV nel payload JSON corretto (inclusa la rinomina di `codiceFiscaleCliente`).
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
