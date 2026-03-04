from client import RestClient
import pandas as pd
import time

# Inizializzazione client
client = RestClient("mail", "password")

# --- Nuove variabili per il controllo del paging ---
enable_paging = True  # Imposta a False per disabilitare il paging e fare una singola chiamata
fixed_call_limit = 10 # Se enable_paging è False, imposta il numero di record nella singola chiamata
# --------------------------------------------------

all_results = [] # Lista per accumulare tutti i record
limit = 1000    # Numero di record per chiamata (questo sarà il limite per ogni pagina o per la singola chiamata se paging disabilitato)
offset = 0       # Punto di partenza
continue_paging = True

print("Inizio recupero dati...")

# Se il paging è disabilitato, impostiamo il limite per la singola chiamata
if not enable_paging:
    limit = fixed_call_limit
    print(f"Paging disabilitato. Effettuerò una singola chiamata con limite: {limit}")

while continue_paging:
    post_data = dict()
    post_data[0] = dict(
        target="eshop.wuerth.it",
        location_name="Italy",
        language_name="Italian",
        historical_serp_mode="all",
        filters=[
            ["keyword_data.keyword_info.search_volume", ">", 10],
            "and",
            [
                ["ranked_serp_element.serp_item.type", "<>", "paid"],
                "or",
                ["ranked_serp_element.serp_item.is_paid", "=", False]
            ]
        ],
        limit=limit,
        offset=offset
    )

    # Chiamata API
    response = client.post("/v3/dataforseo_labs/google/ranked_keywords/live", post_data)

    if response["status_code"] == 20000:
        # Estrazione dei dati (DataForSEO restituisce i risultati nel primo task)
        task_result = response['tasks'][0]['result']

        # Verifica se 'items' esiste e non è nullo
        items = task_result[0].get('items') if task_result and task_result[0] else None

        if items:
            count = len(items)
            all_results.extend(items)
            print(f"Recuperati {count} record (Offset: {offset}). Totale accumulato: {len(all_results)}")

            # Logica per continuare o fermare il paging
            if enable_paging:
                # Se abbiamo ricevuto meno record del limite, significa che abbiamo finito le pagine
                if count < limit:
                    print("Fine dei dati raggiunta (paging abilitato).")
                    continue_paging = False
                else:
                    # Incrementa l'offset per la prossima chiamata
                    offset += limit
                    # Piccola pausa per non sovraccaricare (opzionale)
                    time.sleep(0.5)
            else: # Paging disabilitato, esegui una sola chiamata
                print("Paging disabilitato. Eseguita una singola chiamata.")
                continue_paging = False # Assicura che si fermi dopo la prima chiamata
        else:
            print("Nessun altro dato trovato.")
            continue_paging = False
    else:
        print(f"Errore API alla quota {offset}. Code: {response['status_code']} - {response['status_message']}")
        continue_paging = False

# Una volta terminato il ciclo, gestiamo l'export
if all_results:
    print(f"Creazione file CSV con {len(all_results)} record...")

    # Appiattiamo la struttura JSON nidificata
    df = pd.json_normalize(all_results)

    # Export finale
    filename = "ranked_keywords_wuerth_historical.csv"
    df.to_csv(filename, index=False, encoding='utf-8-sig')

    print(f"Esportazione completata: {filename}")
else:
    print("Nessun dato da esportare.")
