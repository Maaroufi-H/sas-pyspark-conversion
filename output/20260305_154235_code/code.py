# =============================================================
# Script generato automaticamente da convert_engine.py
# SAS originale : /home/user/sas-pyspark-conversion/INPUT/code.sas
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
# %MACRO aggregazione(livello) [riga 2–160]
# ══════════════════════════════════════════════════════
def macro_aggregazione(livello, spark):
    # ============================================================
    # TODO: REVISIONE MANUALE NECESSARIA
    # Motivo    : SQL dinamico: macro SAS nel corpo della query (%if/%then/%do, %str, %sysfunc, call execute).
    # Il codice SAS genera SQL diverso a runtime in base a variabili macro.
    # Strategia: risolvere le variabili macro prima di convertire,
    # oppure usare una logica if/else Python che chiama spark.sql() distinte.
    # Categoria : PROC_SQL
    # Righe     : 16 – 49
    # ============================================================
    # Codice SAS originale:
    #   proc sql noprint;
    #   select distinct prodotto_DESC  into :prod1- from dati_dw.prodotto_canale
    #   where prodotto_cod in ( %if &nid = 1 %then %do;
    #   								 "&id_prodotto1."
    #   								%end;
    #   
    #   								%else %do;
    #   									%do i=1 %to &nid;
    #   
    #   										%if &i ^= &nid %then %do;
    #   											"&&id_prodotto&i",
    #   										%end;
    #   
    #   										%else %do;
    #   											"&&id_prodotto&i"
    #   										%end;
    #   
    #   									%end;
    #   								%end;
    #   							   )
    #   ;
    #   %let N_prod = &sqlobs;
    #   select distinct %if &livello = 1 %then %do;
    #   				canale_aggr
    #   				%end;
    #   				%if &livello = 2 %then %do;
    #   				aggreg
    #   				%end;
    #   				%if &livello = 3 %then %do;
    #   				aggreg2
    #   ... (troncato)
    # ============================================================
    # SQL originale (con macro SAS):
    #   select distinct prodotto_DESC  into :prod1- from dati_dw.prodotto_canale
    #   where prodotto_cod in ( %if &nid = 1 %then %do;
    #   								 "&id_prodotto1."
    #   								%end;
    #   
    #   								%else %do;
    #   									%do i=1 %to &nid;
    #   										
    #   										%if &i ^= &nid %then %do;
    #   											"&&id_prodotto&i",
    #   										%end;
    #   
    #   										%else %do;
    #   											"&&id_prodotto&i"
    #   										%end;
    #   
    #   									%end;
    #   								%end;
    #   							   )
    #   ;
    #   %let N_prod = &sqlobs;
    #   select distinct %if &livello = 1 %then %do;
    #   				canale_aggr
    #   				%end;
    #   				%if &livello = 2 %then %do;
    # ── DATA: visualizza&i [riga 53–77] ──
    visualizza_i = (
        spark.table("PERDITE_UOCE_AGGR")
                .filter(F.expr("prodotto_DESC = "TODO_MACRO_VAR_TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
                .withColumn("prodotto", F.expr(""TODO_MACRO_VAR_TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
                .withColumn("prodotto", F.expr(""TODO_MACRO_VAR_TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
    )
    # ── DATA: APPOGGIO [riga 80–82] ──
    appoggio = spark.table("%do")
    # ============================================================
    # TODO: REVISIONE MANUALE NECESSARIA
    # Motivo    : SQL dinamico: macro SAS nel corpo della query (%if/%then/%do, %str, %sysfunc, call execute).
    # Il codice SAS genera SQL diverso a runtime in base a variabili macro.
    # Strategia: risolvere le variabili macro prima di convertire,
    # oppure usare una logica if/else Python che chiama spark.sql() distinte.
    # Categoria : PROC_SQL
    # Righe     : 84 – 120
    # ============================================================
    # Codice SAS originale:
    #   proc sql noprint;
    #   create table TAB_PERDITE&LIVELLO as
    #   select distinct t2.aggregazione, t1.prodotto,
    #   %do i = 1 %to &N_col;
    #   	%if &i = &N_col %then %do;
    #   		sum(t1.&&col&i) as &&col&i
    #   	%end;
    #   
    #   	%else %do;
    #   		sum(t1.&&col&i) as &&col&i,
    #   	%end;
    #   %end;
    #   from APPOGGIO t1
    #   left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
    #   group by t1.prodotto, t2.aggregazione
    #   ;
    #   /*
    #   create table riassunto as
    #   select distinct aggregazione,
    #   %do i = 1 %to &N_col;
    #   	sum(&&col&i) as &&col&i,
    #   %end;
    #   sum(%do i = 1 %to &N_col;
    #   		%if &i = &N_col %then %do;
    #   			sum(&&col&i))
    #   		%end;
    #   		%else %do;
    #   			sum(&&col&i),
    #   		%end;
    #   	%end; as TOT
    #   ... (troncato)
    # ============================================================
    # SQL originale (con macro SAS):
    #   create table TAB_PERDITE&LIVELLO as
    #   select distinct t2.aggregazione, t1.prodotto,
    #   %do i = 1 %to &N_col;
    #   	%if &i = &N_col %then %do;
    #   		sum(t1.&&col&i) as &&col&i
    #   	%end;
    #   
    #   	%else %do;
    #   		sum(t1.&&col&i) as &&col&i, 
    #   	%end;
    #   %end;
    #   from APPOGGIO t1
    #   left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
    #   group by t1.prodotto, t2.aggregazione
    #   ;
    #   /*
    #   create table riassunto as
    #   select distinct aggregazione,
    #   %do i = 1 %to &N_col;
    #   	sum(&&col&i) as &&col&i, 
    #   %end;
    #   sum(%do i = 1 %to &N_col;
    #   		%if &i = &N_col %then %do;
    #   			sum(&&col&i))
    #   		%end; 

# ══════════════════════════════════════════════════════
# %MACRO varie_aggregazioni() [riga 162–176]
# ══════════════════════════════════════════════════════
def macro_varie_aggregazioni(spark):
    # %IF &livAgregg_count = 1 [riga 164–167]
    if TODO_MACRO_VAR_LIVAGREGG_COUNT = 1:
        # ── MACRO CALL %aggregazione [riga 166] ──
        macro_aggregazione(livello = TODO_MACRO_VAR_LIV, spark=spark)
    # %ELSE [riga 169–173]
    else:
        # ── MACRO CALL %aggregazione [riga 172] ──
        macro_aggregazione(livello = TODO_MACRO_VAR_LIV, spark=spark)

# ── PROC SQL [riga 184–188] ──
# INTO :id_prodotto1 → variable Python
_df_into = spark.sql("""select distinct id 
into :id_prodotto1-
from prodIn_cruscSettimanale;""")
id_prodotto1 = _df_into.collect()[0][0]
# TODO: verifica SQL e nomi tabelle/alias

# ══════════════════════════════════════════════════════
# %MACRO vari_join() [riga 192–240]
# ══════════════════════════════════════════════════════
def macro_vari_join(spark):
    # ── PROC SQL [riga 194–225] ──
    # LISTA IN generata da macro SAS per la colonna 'prodotto_code':
    # SAS usa un loop %do per costruire i valori a runtime.
    # Variabili macro da risolvere: &i, &id_prodotto, &id_prodotto1, &nid
    _prodotto_code_values = []  # TODO: popola con i valori delle variabili macro
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
	where t1.prodotto_code in ( %if &nid = 1 %then %do;
								 "&id_prodotto1."
								%end;

								%else %do;
									%do i=1 %to &nid;
										
										%if &i ^= &nid %then %do;
											"&&id_prodotto&i",
										%end;

										%else %do;
											"&&id_prodotto&i"
										%end;

									%end;
								%end;
							   )
	;""")
    perdite_uoce = perdite_uoce.filter(F.col("prodotto_code").isin(_prodotto_code_values))
    perdite_uoce.write.mode("overwrite").saveAsTable("PERDITE_UOCE")
    # TODO: verifica SQL e nomi tabelle/alias
    # ── DATA: PERDITE_UOCE [riga 227–238] ──
    perdite_uoce = spark.table("PERDITE_UOCE")

# ══════════════════════════════════════════════════════
# %MACRO perdite_frodi() [riga 242–342]
# ══════════════════════════════════════════════════════
def macro_perdite_frodi(spark):
    # ── PROC SQL [riga 257–282] ──
    # LISTA IN generata da macro SAS per la colonna 'prodotto_cod':
    # SAS usa un loop %do per costruire i valori a runtime.
    # Variabili macro da risolvere: &i, &id_prodotto, &id_prodotto1, &nid
    _prodotto_cod_values = []  # TODO: popola con i valori delle variabili macro
    # NOTA: le condizioni IN con macro SAS sono applicate via .filter().isin() dopo la query
    # INTO :prod1 → variable Python
    _df_into = spark.sql("""select distinct prodotto_DESC  into :prod1- from dati_dw.prodotto_canale
	;
	%let N_prod = &sqlobs;
	select distinct &&campo&k.. into :col1- from PERDITE_UOCE
	;
	%let N_col = &sqlobs;""")
    prod1 = _df_into.collect()[0][0]
    # TODO: verifica SQL e nomi tabelle/alias
    # ── DATA: visualizza&i [riga 285–314] ──
    visualizza_i = (
        spark.table("PERDITE_UOCE_AGGR")
                .filter(F.expr("prodotto_DESC = "TODO_MACRO_VAR_TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
                .withColumn("prodotto", F.expr(""TODO_MACRO_VAR_TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
                .withColumn("prodotto", F.expr(""TODO_MACRO_VAR_TODO_MACRO_VAR_PRODTODO_MACRO_VAR_I""))
    )
    # ── DATA: APPOGGIO [riga 317–319] ──
    appoggio = spark.table("%do")
    # ============================================================
    # TODO: REVISIONE MANUALE NECESSARIA
    # Motivo    : SQL dinamico: macro SAS nel corpo della query (%if/%then/%do, %str, %sysfunc, call execute).
    # Il codice SAS genera SQL diverso a runtime in base a variabili macro.
    # Strategia: risolvere le variabili macro prima di convertire,
    # oppure usare una logica if/else Python che chiama spark.sql() distinte.
    # Categoria : PROC_SQL
    # Righe     : 321 – 339
    # ============================================================
    # Codice SAS originale:
    #   	proc sql noprint;
    #   	create table PERDITE_&&campo&k.. as
    #   	select distinct t2.aggregazione, t1.prodotto,
    #   	%do i = 1 %to &N_col;
    #   		%if &i = &N_col %then %do;
    #   			sum(t1.&&col&i) as &&col&i
    #   		%end;
    #   
    #   		%else %do;
    #   			sum(t1.&&col&i) as &&col&i,
    #   		%end;
    #   	%end;
    #   	from APPOGGIO t1
    #   	left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
    #   	group by t1.prodotto, t2.aggregazione
    #   	;
    #   
    #   	proc delete data = PERDITE_UOCE_AGGR APPOGGIO %do i = 1 %to &N_prod; visualizza&i %end; ;
    #   	run;
    # ============================================================
    # SQL originale (con macro SAS):
    #   create table PERDITE_&&campo&k.. as
    #   	select distinct t2.aggregazione, t1.prodotto,
    #   	%do i = 1 %to &N_col;
    #   		%if &i = &N_col %then %do;
    #   			sum(t1.&&col&i) as &&col&i
    #   		%end;
    #   
    #   		%else %do;
    #   			sum(t1.&&col&i) as &&col&i, 
    #   		%end;
    #   	%end;
    #   	from APPOGGIO t1
    #   	left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
    #   	group by t1.prodotto, t2.aggregazione
    #   	;
    #   	
    #   	proc delete data = PERDITE_UOCE_AGGR APPOGGIO %do i = 1 %to &N_prod; visualizza&i %end; ;

# ── PROC SQL [riga 352–356] ──
# INTO :campo1 → variable Python
_df_into = spark.sql("""select distinct campo
into :campo1-
from var_frodi_reclami;""")
campo1 = _df_into.collect()[0][0]
# TODO: verifica SQL e nomi tabelle/alias
