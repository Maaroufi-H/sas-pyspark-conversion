# =============================================================
# Script generato automaticamente da convert_engine.py
# SAS originale : /home/user/sas-pyspark-conversion/output/20260309_121220_code/code.resolved.sas
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
# %MACRO aggregazione(livello) [riga 2–177]
# ══════════════════════════════════════════════════════
def macro_aggregazione(livello, spark):
    # ============================================================
    # TODO: REVISIONE MANUALE NECESSARIA
    # Motivo    : SQL dinamico: macro SAS nel corpo della query (%if/%then/%do, %str, %sysfunc, call execute).
    # Il codice SAS genera SQL diverso a runtime in base a variabili macro.
    # Strategia: risolvere le variabili macro prima di convertire,
    # oppure usare una logica if/else Python che chiama spark.sql() distinte.
    # Categoria : PROC_SQL
    # Righe     : 16 – 57
    # ============================================================
    # Codice SAS originale:
    #   proc sql noprint;
    #   select distinct prodotto_DESC  into :prod1- from dati_dw.prodotto_canale
    #   where prodotto_cod in ( %if 3 = 1 %then %do;
    #   								 "&id_prodotto1."
    #   								%end;
    #   
    #   								%else %do;
    #   
    #   
    #   										%if 1 ^= 3 %then %do;
    #   											"1d_prodotto1",
    #   
    #   
    #   										%if 2 ^= 3 %then %do;
    #   											"2d_prodotto2",
    #   
    #   
    #   										%if 3 ^= 3 %then %do;
    #   											"3d_prodotto3",
    #   
    #   
    #   										%else %do;
    #   											"&id_prodotto&i"
    #   										%end;
    #   
    #   									%end;
    #   								%end;
    #   							   )
    #   ;
    #   %let N_prod = &sqlobs;
    #   ... (troncato)
    # ============================================================
    # SQL originale (con macro SAS):
    #   select distinct prodotto_DESC  into :prod1- from dati_dw.prodotto_canale
    #   where prodotto_cod in ( %if 3 = 1 %then %do;
    #   								 "&id_prodotto1."
    #   								%end;
    #   
    #   								%else %do;
    #   									
    #   										
    #   										%if 1 ^= 3 %then %do;
    #   											"1d_prodotto1",
    #   										
    #   										
    #   										%if 2 ^= 3 %then %do;
    #   											"2d_prodotto2",
    #   										
    #   										
    #   										%if 3 ^= 3 %then %do;
    #   											"3d_prodotto3",
    #   										
    #   
    #   										%else %do;
    #   											"&id_prodotto&i"
    #   										%end;
    #   
    #   									%end;
    # ── DATA: visualizza&i [riga 62–89] ──
    visualizza_i = (
        spark.table("PERDITE_UOCE_AGGR")
                .filter(F.expr("prodotto_DESC = "TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
                .withColumn("prodotto", F.expr(""TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
                .withColumn("prodotto", F.expr(""TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
    )
    # ── DATA: APPOGGIO [riga 92–95] ──
    appoggio = spark.table("/*")
    # ============================================================
    # TODO: REVISIONE MANUALE NECESSARIA
    # Motivo    : SQL dinamico: macro SAS nel corpo della query (%if/%then/%do, %str, %sysfunc, call execute).
    # Il codice SAS genera SQL diverso a runtime in base a variabili macro.
    # Strategia: risolvere le variabili macro prima di convertire,
    # oppure usare una logica if/else Python che chiama spark.sql() distinte.
    # Categoria : PROC_SQL
    # Righe     : 97 – 133
    # ============================================================
    # Codice SAS originale:
    #   proc sql noprint;
    #   create table TAB_PERDITE&LIVELLO as
    #   select distinct t2.aggregazione, t1.prodotto,
    #   /* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */
    #   
    #   	%if &i = &sqlobs %then %do;
    #   		sum(t1.&col&i) as &col&i
    #   	%end;
    #   
    #   	%else %do;
    #   		sum(t1.&col&i) as &col&i,
    #   	%end;
    #   %end;
    #   from APPOGGIO t1
    #   left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
    #   group by t1.prodotto, t2.aggregazione
    #   ;
    #   /*
    #   create table riassunto as
    #   select distinct aggregazione,
    #   /* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */
    #   
    #   	sum(&col&i) as &col&i,
    #   %end;
    #   sum(/* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */
    #   
    #   		%if &i = &sqlobs %then %do;
    #   			sum(&col&i))
    #   		%end;
    #   		%else %do;
    #   ... (troncato)
    # ============================================================
    # SQL originale (con macro SAS):
    #   create table TAB_PERDITE&LIVELLO as
    #   select distinct t2.aggregazione, t1.prodotto,
    #   /* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */
    #   
    #   	%if &i = &sqlobs %then %do;
    #   		sum(t1.&col&i) as &col&i
    #   	%end;
    #   
    #   	%else %do;
    #   		sum(t1.&col&i) as &col&i, 
    #   	%end;
    #   %end;
    #   from APPOGGIO t1
    #   left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
    #   group by t1.prodotto, t2.aggregazione
    #   ;
    #   /*
    #   create table riassunto as
    #   select distinct aggregazione,
    #   /* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */
    #   
    #   	sum(&col&i) as &col&i, 
    #   %end;
    #   sum(/* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */
    #   

# ══════════════════════════════════════════════════════
# %MACRO varie_aggregazioni() [riga 179–193]
# ══════════════════════════════════════════════════════
def macro_varie_aggregazioni(spark):
    # %IF &livAgregg_count = 1 [riga 181–184]
    if TODO_MACRO_VAR_LIVAGREGG_COUNT = 1:
        # ── MACRO CALL %aggregazione [riga 183] ──
        macro_aggregazione(livello = %sysfunc(int(1, spark=spark)
    # %ELSE [riga 186–190]
    else:
        # ── MACRO CALL %aggregazione [riga 189] ──
        macro_aggregazione(livello = %sysfunc(int(1, spark=spark)

# ── PROC SQL [riga 201–205] ──
# INTO :id_prodotto1-  →  lista Python + contatore (equivalente a &sqlobs)
_df_into = spark.sql("""select distinct id 
from prodIn_cruscSettimanale;""")
id_prodotto_list = [row[0] for row in _df_into.collect()]
n_id_prodotto = len(id_prodotto_list)
# Accesso singolo elemento: id_prodotto_list[0], id_prodotto_list[1], ...
# TODO: verifica SQL e nomi tabelle/alias

# ══════════════════════════════════════════════════════
# %MACRO vari_join() [riga 209–257]
# ══════════════════════════════════════════════════════
def macro_vari_join(spark):
    # ── PROC SQL [riga 211–242] ──
    # LISTA IN per 'prodotto_code': generata da macro SAS a runtime.
    # Variabili macro da risolvere: &i, &id_prodotto, &id_prodotto1
    # Popola questa lista con i valori corrispondenti ai parametri SAS.
    _prodotto_code_values = []  # TODO: popola con i valori di &i, &id_prodotto, &id_prodotto1
    # NOTA: le condizioni IN con macro SAS sono applicate via .filter().isin() dopo la query
    perdite_uoce = spark.sql("""create table perdite_uoce as
	select t1.*,
	t2.riferimento /*as aggregaz*/,
	t2.aggregazione as riassunto,
	t3.cruscotto as aggreg1,
	t4.aggreg as aggreg2,
	t4.aggreg2 as aggreg3
	from QUERY_FOR_PERDITEUOCE t1 
	left join DATI_DW.prodotto_canale t2 on (t1.prodotto_code = t2.prodotto_cod)
	left join DATI_DW.cod_frodi t3 on (t1.Event_Type_CODE = t3.Event_Type)
	left join dati_dw.aggregazioni t4 on (t3.cruscotto = t4.canale_aggr)
	where t1.prodotto_code in ( %if 3 = 1 %then %do;
								 "&id_prodotto1."
								%end;

								%else %do;
									%do i=1 %to 3;
										
										%if &i ^= 3 %then %do;
											"&id_prodotto&i",
										%end;

										%else %do;
											"&id_prodotto&i"
										%end;

									%end;
								%end;
							   )
	;""")
    perdite_uoce = perdite_uoce.filter(F.col("prodotto_code").isin(_prodotto_code_values))
    perdite_uoce.write.mode("overwrite").saveAsTable("PERDITE_UOCE")
    # TODO: verifica SQL e nomi tabelle/alias
    # ── DATA: PERDITE_UOCE [riga 244–255] ──
    perdite_uoce = spark.table("PERDITE_UOCE")

# ══════════════════════════════════════════════════════
# %MACRO perdite_frodi() [riga 259–359]
# ══════════════════════════════════════════════════════
def macro_perdite_frodi(spark):
    # ── PROC SQL [riga 274–299] ──
    # LISTA IN per 'prodotto_cod': generata da macro SAS a runtime.
    # Variabili macro da risolvere: &i, &id_prodotto, &id_prodotto1
    # Popola questa lista con i valori corrispondenti ai parametri SAS.
    _prodotto_cod_values = []  # TODO: popola con i valori di &i, &id_prodotto, &id_prodotto1
    # NOTA: le condizioni IN con macro SAS sono applicate via .filter().isin() dopo la query
    # INTO :prod1-  →  lista Python + contatore (equivalente a &sqlobs)
    _df_into = spark.sql("""select distinct prodotto_DESC  from dati_dw.prodotto_canale
	;
	%let N_prod = &sqlobs;
	select distinct &campo&k.. from PERDITE_UOCE
	;
	%let N_col = &sqlobs;""")
    prod_list = [row[0] for row in _df_into.collect()]
    n_prod = len(prod_list)
    # Accesso singolo elemento: prod_list[0], prod_list[1], ...
    # TODO: verifica SQL e nomi tabelle/alias
    # ── DATA: visualizza&i [riga 302–331] ──
    visualizza_i = (
        spark.table("PERDITE_UOCE_AGGR")
                .filter(F.expr("prodotto_DESC = "TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
                .withColumn("prodotto", F.expr(""TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
                .withColumn("prodotto", F.expr(""TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
    )
    # ── DATA: APPOGGIO [riga 334–336] ──
    appoggio = spark.table("%do")
    # ============================================================
    # TODO: REVISIONE MANUALE NECESSARIA
    # Motivo    : SQL dinamico: macro SAS nel corpo della query (%if/%then/%do, %str, %sysfunc, call execute).
    # Il codice SAS genera SQL diverso a runtime in base a variabili macro.
    # Strategia: risolvere le variabili macro prima di convertire,
    # oppure usare una logica if/else Python che chiama spark.sql() distinte.
    # Categoria : PROC_SQL
    # Righe     : 338 – 356
    # ============================================================
    # Codice SAS originale:
    #   	proc sql noprint;
    #   	create table PERDITE_&campo&k.. as
    #   	select distinct t2.aggregazione, t1.prodotto,
    #   	%do i = 1 %to &sqlobs;
    #   		%if &i = &sqlobs %then %do;
    #   			sum(t1.&col&i) as &col&i
    #   		%end;
    #   
    #   		%else %do;
    #   			sum(t1.&col&i) as &col&i,
    #   		%end;
    #   	%end;
    #   	from APPOGGIO t1
    #   	left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
    #   	group by t1.prodotto, t2.aggregazione
    #   	;
    #   
    #   	proc delete data = PERDITE_UOCE_AGGR APPOGGIO %do i = 1 %to &sqlobs; visualizza&i %end; ;
    #   	run;
    # ============================================================
    # SQL originale (con macro SAS):
    #   create table PERDITE_&campo&k.. as
    #   	select distinct t2.aggregazione, t1.prodotto,
    #   	%do i = 1 %to &sqlobs;
    #   		%if &i = &sqlobs %then %do;
    #   			sum(t1.&col&i) as &col&i
    #   		%end;
    #   
    #   		%else %do;
    #   			sum(t1.&col&i) as &col&i, 
    #   		%end;
    #   	%end;
    #   	from APPOGGIO t1
    #   	left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
    #   	group by t1.prodotto, t2.aggregazione
    #   	;
    #   	
    #   	proc delete data = PERDITE_UOCE_AGGR APPOGGIO %do i = 1 %to &sqlobs; visualizza&i %end; ;

# ── PROC SQL [riga 369–373] ──
# INTO :campo1-  →  lista Python + contatore (equivalente a &sqlobs)
_df_into = spark.sql("""select distinct campo
from var_frodi_reclami;""")
campo_list = [row[0] for row in _df_into.collect()]
n_campo = len(campo_list)
# Accesso singolo elemento: campo_list[0], campo_list[1], ...
# TODO: verifica SQL e nomi tabelle/alias
