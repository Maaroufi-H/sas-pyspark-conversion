# =============================================================
# Script generato automaticamente da convert_engine.py
# SAS originale : /home/user/sas-pyspark-conversion/test_macro_in_list.sas
# Generato da   : run_converter.py
# SAS → PySpark  |  2026-03-03
# Richiede : PySpark, delta (opzionale)
# =============================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, DateType, TimestampType
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("sas_converted").getOrCreate()


# ══════════════════════════════════════════════════════
# %MACRO filtra_prodotti(nid, id_prodotto1, id_prodotto2, id_prodotto3) [riga 1–25]
# ══════════════════════════════════════════════════════
def macro_filtra_prodotti(nid, id_prodotto1, id_prodotto2, id_prodotto3, spark):
    # ── PROC SQL [riga 3–23] ──
    # LISTA IN generata da macro SAS per la colonna 'prodotto_cod':
    # SAS usa un loop %do per costruire i valori a runtime.
    # Variabili macro da risolvere: &i, &id_prodotto, &id_prodotto1, &nid
    _prodotto_cod_values = []  # TODO: popola con i valori delle variabili macro
    # NOTA: le condizioni IN con macro SAS sono applicate via .filter().isin() dopo la query
    work_risultato = spark.sql("""create table work.risultato as
    select *
    from source.vendite
    where  data_vendita >= '2024-01-01';""")
    work_risultato = work_risultato.filter(F.col("prodotto_cod").isin(_prodotto_cod_values))
    work_risultato.write.mode("overwrite").saveAsTable("work.risultato")
    # TODO: verifica SQL e nomi tabelle/alias
