import csv
import json
import requests
from datetime import datetime

# --- CONFIGURAZIONE ---
CLIENT_ID = "IL_TUO_CLIENT_ID"
CLIENT_SECRET = "IL_TUO_CLIENT_SECRET"
# URL Token come da specifiche (Source 127)
TOKEN_URL = "https://login.microsoftonline.com/afd0a75c-8671-4cce-9061-2ca0d92e422f/oauth2/v2.0/token"
# URL Base (Sostituire con l'ambiente corretto, es. collaudo o produzione)
BASE_URL = "https://api.invitalia.it/v1"
CSV_FILE = "dati.csv"
OUTPUT_JSON_FILE = "risposte_api.json"


def get_bearer_token():
  """Recupera il token Bearer tramite chiamata GET"""
  # Parametri passati in query string per la GET
  params = {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'scope': f'{CLIENT_ID}/.default'
  }

  try:
    # Modificato in GET come richiesto
    response = requests.get(TOKEN_URL, params=params)
    response.raise_for_status()
    return response.json().get('access_token')
  except requests.exceptions.RequestException as e:
    print(f"Errore recupero Token: {e}")
    try:
      print(response.text)
    except:
      pass
    exit(1)


def format_date(date_str):
  """
  Converte la data dal formato CSV '09/12/2025, 09:56:41,029'
  [cite_start]al formato ISO 8601 'yyyyMMddTHH:mm:ssZ' richiesto[cite: 132, 133].
  """
  try:
    clean_date = date_str.split(',')[0] + " " + date_str.split(',')[1].strip()
    dt_obj = datetime.strptime(clean_date, "%d/%m/%Y %H:%M:%S")
    return dt_obj.strftime("%Y%m%dT%H:%M:%SZ")
  except Exception as e:
    print(f"Errore parsing data '{date_str}': {e}")
    return date_str


def process_csv():
  token = get_bearer_token()
  headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
  }

  # Endpoint aggiornato a "erogazioni"
  url_erogazioni = f"{BASE_URL}/erogazioni"

  # Lista per accumulare tutte le risposte
  all_responses_log = []

  try:
    with open(CSV_FILE, mode='r', encoding='utf-8-sig') as f:
      reader = csv.DictReader(f)

      for row in reader:
        try:
          # Conversione importo: da centesimi (int) a euro (float)
          importo_centesimi = float(row['importo'])
          importo_reale = importo_centesimi / 100

          payload = {
            "requestId": row['id'],
            "anagrafica": {
              "partitaIvaCliente": row['partitaIvaCliente'],
              "codiceFiscaleCliente": row['codiceFiscaleCliente'],
              "ragioneSocialeIntestatario": row['ragioneSocialeIntestatario'],
              "pec": row['pec'],
              "indirizzo": row['indirizzo'],
              "cap": row['cap'],
              "localita": row['localita'],
              "provincia": row['provincia']
            },
            "erogazione": {
              "idPratica": row['idPratica'],
              "dataAmmissione": format_date(row['dataAmmissione']),
              "ibanBeneficiario": row['ibanBeneficiario'],
              "importo": importo_reale,
              "intestatarioContoCorrente": row['intestatarioContoCorrente'],
              "autorizzatore": row['autorizzatore']
            }
          }

          # [cite_start]Request-Id nell'header [cite: 123]
          headers['Request-Id'] = row['id']

          print(
            f"Invio pratica {row['idPratica']} (Importo: {importo_reale})...")

          # Chiamata POST
          response = requests.post(url_erogazioni, headers=headers,
                                   json=payload)

          # Gestione risposta
          response_data = {
            "csv_id": row['id'],
            "idPratica": row['idPratica'],
            "status_code": response.status_code,
            "timestamp": datetime.now().isoformat()
          }

          # Tentativo di parsing JSON della risposta, altrimenti testo semplice
          try:
            response_data["api_response"] = response.json()
          except json.JSONDecodeError:
            response_data["api_response"] = response.text

          # Aggiungo alla lista cumulativa
          all_responses_log.append(response_data)

          # Verifica successo (Qualsiasi 2xx)
          if 200 <= response.status_code < 300:
            print(f"--> SUCCESSO ({response.status_code})")
          else:
            print(f"--> ERRORE ({response.status_code})")

        except Exception as e:
          print(f"Errore elaborazione riga ID {row.get('id', 'N/A')}: {e}")
          all_responses_log.append({
            "csv_id": row.get('id', 'unknown'),
            "error": str(e)
          })

  except FileNotFoundError:
    print(f"File {CSV_FILE} non trovato.")
    return

  # Salvataggio unico file JSON finale
  with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f_out:
    json.dump(all_responses_log, f_out, indent=4, ensure_ascii=False)

  print(f"\nElaborazione completata. Risposte salvate in '{OUTPUT_JSON_FILE}'")


if __name__ == "__main__":
  process_csv()