import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def import_csv(file_path, target_table):
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASS")
    )
    cur = conn.cursor()

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Utilisation de copy_expert pour une insertion ultra-rapide (méthode recommandée)
            sql = f'COPY "{target_table}" FROM STDIN WITH CSV HEADER DELIMITER \',\' QUOTE \'"\''
            cur.copy_expert(sql, f)
        
        conn.commit()
        print(f"✅ Importation de {file_path} réussie dans {target_table}.")
    except Exception as e:
        print(f"❌ Erreur lors de l'importation CSV : {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # Exemple d'usage : 
    # import_csv('chemin/vers/ton/dump_voi.csv', 'MICROMOBILITY_RAW.VOI_TRIPS_HISTORIC')
    print("Prêt pour l'import.")