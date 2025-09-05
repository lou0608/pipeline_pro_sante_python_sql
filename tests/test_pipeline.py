import sqlite3
import pandas as pd


def test_pipeline_loads_data(tmp_path):
    # Simule un mini DataFrame comme le ferait staging_loader
    df = pd.DataFrame({
        "annee": [2022, 2022],
        "Region": ["Occitanie", "Île-de-France"],
        "Departement": ["31", "75"],
        "profession": ["Médecins", "Infirmiers"],
        "classe_age": ["30-39 ans", "40-49 ans"],
        "Genre": ["Femmes", "Hommes"],
        "Effectif": [123, 456]
    })

    # Connexion SQLite en fichier temporaire
    db_file = tmp_path / "test.db"
    conn = sqlite3.connect(db_file)

    # Insertion en staging
    df.to_sql("stg_pro_sante_raw", conn, if_exists="replace", index=False)

    # Vérification
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stg_pro_sante_raw;")
    count = cur.fetchone()[0]
    conn.close()

    assert count == 2, "La table staging devrait contenir 2 lignes"
