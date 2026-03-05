# =============================================================
# Script generato automaticamente da convert_engine.py
# SAS originale : /home/user/sas-pyspark-conversion/INPUT/import.sas
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


# ── %INCLUDE "/sas/staging/opt/sasop/pgm/SAS_standard/sascode/Poste_RAFA/autoexec.sas" [riga 1] ──
# TODO: importa o converti il file esterno:
#   exec(open("/sas/staging/opt/sasop/pgm/SAS_standard/sascode/Poste_RAFA/autoexec.sas").read())  # oppure: import modulo

# ══════════════════════════════════════════════════════
# %MACRO import(flusso) [riga 3–112]
# ══════════════════════════════════════════════════════
def macro_import(flusso, spark):
    # %IF %sysfunc(exist(result.register)) = 0 [riga 5–16]
    if not spark.catalog.tableExists("result.register"):
        # ── DATA: result.register [riga 7–14] ──
        result_register = spark.createDataFrame([], schema=StructType([
            StructField("computing_datetime", DoubleType(), True),
            StructField("fase", StringType(), True),
            StructField("msg_error", StringType(), True),
            StructField("tipo", StringType(), True),
            StructField("flusso", StringType(), True),
        ]))
        result_register.write.mode("overwrite").saveAsTable("result.register")
    # ============================================================
    # TODO: REVISIONE MANUALE NECESSARIA
    # Motivo    : Pattern DATA step non riconosciuto
    # Categoria : DATA_STEP
    # Righe     : 18 – 26
    # ============================================================
    # Codice SAS originale:
    #   data register;
    #   	length computing_datetime 8 fase $50 msg_error $150 tipo $32 flusso $32;
    #   	computing_datetime = "&timest_for."dt;
    #   	fase = "INIZIO: CARICAMENTO DATI.";
    #   	msg_error = "";
    #   	tipo = "ACQUISIZIONE";
    #   	flusso = "&flusso.";
    #   	format computing_datetime datetime26.;
    #   	run;
    # ============================================================
    # ── PROC APPEND base=result.register data=register [riga 28–29] ──
    result_register = result_register.union(register)
    result_register.write.mode("overwrite").saveAsTable("result.register")
    # ── %INCLUDE "&path./source/Importa_metadati.sas" [riga 32] ──
    # TODO: importa o converti il file esterno:
    #   exec(open("&path./source/Importa_metadati.sas").read())  # oppure: import modulo
    # ── %INCLUDE "&path./source/Importa_liste.sas" [riga 33] ──
    # TODO: importa o converti il file esterno:
    #   exec(open("&path./source/Importa_liste.sas").read())  # oppure: import modulo
    # ── %INCLUDE "&path./source/Acquisisci_flussi.sas" [riga 36] ──
    # TODO: importa o converti il file esterno:
    #   exec(open("&path./source/Acquisisci_flussi.sas").read())  # oppure: import modulo
    # %IF &syscc > 4 [riga 38–72]
    if sys_cc > 4:
        # ── MACRO CALL %mail_errore [riga 42] ──
        macro_mail_errore(import, flusso = flusso, spark=spark)
        # %IF %sysfunc(exist(result.register)) = 0 [riga 44–55]
        if not spark.catalog.tableExists("result.register"):
            # ── DATA: result.register [riga 46–53] ──
            result_register = spark.createDataFrame([], schema=StructType([
                StructField("computing_datetime", DoubleType(), True),
                StructField("fase", StringType(), True),
                StructField("msg_error", StringType(), True),
                StructField("tipo", StringType(), True),
                StructField("flusso", StringType(), True),
            ]))
            result_register.write.mode("overwrite").saveAsTable("result.register")
        # ============================================================
        # TODO: REVISIONE MANUALE NECESSARIA
        # Motivo    : Pattern DATA step non riconosciuto
        # Categoria : DATA_STEP
        # Righe     : 57 – 65
        # ============================================================
        # Codice SAS originale:
        #   		data register;
        #   		length computing_datetime 8 fase $50 msg_error $150 tipo $32 flusso $32;
        #   		computing_datetime = "&timest_for."dt;
        #   		fase = "FINE: CARICAMENTO DATI.";
        #   		msg_error = "ERROR: errore durante il caricamento dei file.";
        #   		tipo = "ACQUISIZIONE";
        #   		flusso = "&flusso.";
        #   		format computing_datetime datetime26.;
        #   		run;
        # ============================================================
        # ── PROC APPEND base=result.register data=register [riga 67–68] ──
        result_register = result_register.union(register)
        result_register.write.mode("overwrite").saveAsTable("result.register")
    # %IF &syscc le 4 and &num_t > 0 [riga 74–106]
    if sys_cc <= 4 and TODO_MACRO_VAR_NUM_T > 0:
        # ── MACRO CALL %mail_caricamento [riga 76] ──
        macro_mail_caricamento(flusso, spark=spark)
        # %IF %sysfunc(exist(result.register)) = 0 [riga 78–89]
        if not spark.catalog.tableExists("result.register"):
            # ── DATA: result.register [riga 80–87] ──
            result_register = spark.createDataFrame([], schema=StructType([
                StructField("computing_datetime", DoubleType(), True),
                StructField("fase", StringType(), True),
                StructField("msg_error", StringType(), True),
                StructField("tipo", StringType(), True),
                StructField("flusso", StringType(), True),
            ]))
            result_register.write.mode("overwrite").saveAsTable("result.register")
        # ============================================================
        # TODO: REVISIONE MANUALE NECESSARIA
        # Motivo    : Pattern DATA step non riconosciuto
        # Categoria : DATA_STEP
        # Righe     : 91 – 99
        # ============================================================
        # Codice SAS originale:
        #   		data register;
        #   		length computing_datetime 8 fase $50 msg_error $150 tipo $32 flusso $32;
        #   		computing_datetime = "&timest_for."dt;
        #   		fase = "FINE: CARICAMENTO DATI.";
        #   		msg_error = "";
        #   		tipo = "ACQUISIZIONE";
        #   		flusso = "&flusso.";
        #   		format computing_datetime datetime26.;
        #   		run;
        # ============================================================
        # ── PROC APPEND base=result.register data=register [riga 101–102] ──
        result_register = result_register.union(register)
        result_register.write.mode("overwrite").saveAsTable("result.register")
        # ── %INCLUDE "&path./source/aggrega_flussi.sas" [riga 104] ──
        # TODO: importa o converti il file esterno:
        #   exec(open("&path./source/aggrega_flussi.sas").read())  # oppure: import modulo
    # ============================================================
    # TODO: REVISIONE MANUALE NECESSARIA
    # Motivo    : PROC DATASETS: gestione catalogo (DELETE/COPY/RENAME) → revisione manuale
    # Categoria : PROC_DATASETS
    # Righe     : 108 – 109
    # ============================================================
    # Codice SAS originale:
    #   	proc datasets lib = work nolist kill;
    #   	run;
    # ============================================================
