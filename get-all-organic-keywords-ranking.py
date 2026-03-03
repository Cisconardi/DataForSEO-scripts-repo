from client import RestClient
import pandas as pd
import time

# Inizializzazione client
client = RestClient("user", "password")

all_results = [] # Lista per accumulare tutti i record
limit = 1000     # Numero di record per chiamata
offset = 0       # Punto di partenza
continue_paging = True

print("Inizio recupero dati...")

while continue_paging:
    post_data = dict()
    post_data[0] = dict(
        target="eshop.wuerth.it",
        location_name="Italy",
        language_name="Italian",
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
    response = client.post("/v3/dataforseo_labs/google/ranked_keywords/all", post_data)

    if response["status_code"] == 20000:
        # Estrazione dei dati (DataForSEO restituisce i risultati nel primo task)
        task_result = response['tasks'][0]['result']
        
        # Verifica se 'items' esiste e non è nullo
        items = task_result[0].get('items') if task_result and task_result[0] else None
        
        if items:
            count = len(items)
            all_results.extend(items)
            print(f"Recuperati {count} record (Offset: {offset}). Totale accumulato: {len(all_results)}")
            
            # Se abbiamo ricevuto meno record del limite, significa che abbiamo finito le pagine
            if count < limit:
                print("Fine dei dati raggiunta.")
                continue_paging = False
            else:
                # Incrementa l'offset per la prossima chiamata
                offset += limit
                # Piccola pausa per non sovraccaricare (opzionale)
                time.sleep(0.5) 
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
    filename = "ranked_keywords_wuerth_full.csv"
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    
    print(f"Esportazione completata: {filename}")
else:
    print("Nessun dato da esportare.")
