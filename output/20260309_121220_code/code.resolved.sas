options mprint;
%MACRO aggregazione(livello);

%put AGGREG&livello;
ods select none;

proc tabulate data=PERDITE_UOCE out=PERDITE_UOCE_AGGR missing;
  class AGGREG&livello prodotto_DESC;
  var SUM_of_SUM_of_Importo;
  table prodotto_DESC,AGGREG&livello*SUM_of_SUM_of_Importo='somma';
run;

ods select all;


proc sql noprint;
select distinct prodotto_DESC  into :prod1- from dati_dw.prodotto_canale
where prodotto_cod in ( %if 3 = 1 %then %do;
								 "&id_prodotto1."
								%end;

								%else %do;
									
										
										%if 1 ^= 3 %then %do;
											"1d_prodotto1",
										
										
										%if 2 ^= 3 %then %do;
											"2d_prodotto2",
										
										
										%if 3 ^= 3 %then %do;
											"3d_prodotto3",
										

										%else %do;
											"&id_prodotto&i"
										%end;

									%end;
								%end;
							   )
;
%let N_prod = &sqlobs;
select distinct %if &livello = 1 %then %do;
				canale_aggr
				%end;
				%if &livello = 2 %then %do;
				aggreg
				%end;
				%if &livello = 3 %then %do;
				aggreg2
				%end;  into :col1- from dati_dw.aggregazioni
;
%let N_col = &sqlobs;
quit;


/* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */

data visualizza&i;
	length prodotto $ 64;

	if _N_ = 1 then do;

		prodotto = "&prod&i";

		/* LOOP_NON_SROTOLATO: %do j = 1 %to &sqlobs; */
 &col&j. = 0; %end;
		output;
	end;
	
	set PERDITE_UOCE_AGGR(where=(prodotto_DESC = "&prod&i"));

	prodotto = "&prod&i";
	/* LOOP_NON_SROTOLATO: %do j = 1 %to &sqlobs; */

			
		&col&j. = 0;
		if aggreg&livello = "&col&j."	then &col&j. = SUM_of_SUM_of_Importo_Sum /1000000;
			
	%end;
	
	KEEP prodotto /* LOOP_NON_SROTOLATO: %do j = 1 %to &sqlobs; */
 &col&j %end;;
	output;
	
run;
%end;

data APPOGGIO;
set /* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */
 visualizza&i %end;;
run;

proc sql noprint;
create table TAB_PERDITE&LIVELLO as
select distinct t2.aggregazione, t1.prodotto,
/* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */

	%if &i = &sqlobs %then %do;
		sum(t1.&col&i) as &col&i
	%end;

	%else %do;
		sum(t1.&col&i) as &col&i, 
	%end;
%end;
from APPOGGIO t1
left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
group by t1.prodotto, t2.aggregazione
;
/*
create table riassunto as
select distinct aggregazione,
/* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */

	sum(&col&i) as &col&i, 
%end;
sum(/* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */

		%if &i = &sqlobs %then %do;
			sum(&col&i))
		%end; 
		%else %do;
			sum(&col&i),
		%end; 
	%end; as TOT
from TAB_PERDITE&LIVELLO
group by aggregazione
;
quit;
*/
proc delete data = PERDITE_UOCE_AGGR APPOGGIO /* LOOP_NON_SROTOLATO: %do i = 1 %to &sqlobs; */
 visualizza&i %end; ;
run;
/*
proc export
	data= TAB_PERDITE&LIVELLO
	dbms=xlsx
    outfile="/sas/staging/opt/sasop/pgm/SAS_standard/sascode/finale.xlsx"
    replace;
run;

proc sql noprint;
create table tab_new as
select
%do i = 1 %to &sqlobs;
	
	sum(&col&i) as &col&i, 

%end;

sum(%do i = 1 %to &sqlobs;
		%if &i = &sqlobs %then %do;
			sum(&col&i))
		%end; 
		%else %do;
			sum(&col&i),
		%end; 
	%end; as TOT
from TAB_PERDITE&LIVELLO
;
quit;

proc transpose data=tab_new out=tab_new_transp;
var %do i = 1 %to &sqlobs; &col&i %end; tot;
run;

data tab_new;
set tab_new_transp(rename=(_NAME_ = perd_op_nette col1 = mln_euro ));
run;

*/
   
%mend;

%macro varie_aggregazioni;

	%if &livAgregg_count = 1 %then %do;
		%let liv = 1; 
		%aggregazione(livello = %sysfunc(int(1)))
	%end;

	%else %do;
		%do k = 1 %to &livAgregg_count;
			%let liv = %sysfunc(int(1&k)); 
			%aggregazione(livello = %sysfunc(int(1)))
		%end;
	%end;

%mend;

proc import OUT= prodIn_cruscSettimanale
            DATAFILE= "&path_excel./prodIn_cruscSettimanale.xlsx" 
            DBMS=XLSX REPLACE;
     		GETNAMES=YES;
run;

proc sql noprint;
select distinct id 
into :id_prodotto1-
from prodIn_cruscSettimanale;
quit;

%let nid = &sqlobs;

%macro vari_join;

	proc sql;
	create table PERDITE_UOCE as
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
	;
	quit;

	data PERDITE_UOCE;
		set PERDITE_UOCE;
		if aggreg1 = "" then aggreg1 = "altro";
		if aggreg2 = "" then aggreg2 = "altro";
		if aggreg3 = "" then aggreg3 = "altro";
		%do i = 1 %to &sqlobs.;

			&campo&i.. = compress(translate(trimn(&campo&i..),"_"," "), , 'kad');
			if &campo&i.. = "" then &campo&i.. = "altro";

		%end;
	run;

%mend vari_join;

%macro perdite_frodi;

%do k = 1 %to &sqlobs.;

	ods select none;

	proc tabulate data=PERDITE_UOCE out=PERDITE_UOCE_AGGR missing;
	  class &campo&k.. prodotto_DESC;
	  var SUM_of_SUM_of_Importo;
	  table prodotto_DESC,&campo&k..*SUM_of_SUM_of_Importo='somma';
	run;

	ods select all;

	
	proc sql noprint;
	select distinct prodotto_DESC  into :prod1- from dati_dw.prodotto_canale
	where prodotto_cod in ( %if 3 = 1 %then %do;
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
	;
	%let N_prod = &sqlobs;
	select distinct &campo&k.. into :col1- from PERDITE_UOCE
	;
	%let N_col = &sqlobs;
	quit;

	%do i = 1 %to &sqlobs;
	data visualizza&i;
		length prodotto $ 64;

		if _N_ = 1 then do;

			prodotto = "&prod&i";

			%do j = 1 %to &sqlobs;
				%let old_col&j. = &col&j.;
				%let col&j. = %sysfunc(compress(&col&j..,, ka));
				%let col&j. = %substr(&col&j.., 1, %sysfunc(min(32, %length(&col&j..))));
				&col&j. = 0; 
			%end;
			output;
		end;
		
		set PERDITE_UOCE_AGGR(where=(prodotto_DESC = "&prod&i"));

		prodotto = "&prod&i";
		%do j = 1 %to &sqlobs;
				
			&col&j. = 0;
			if &campo&k.. = "&old_col&j.."	then &col&j. = SUM_of_SUM_of_Importo_Sum /1000000;
				
		%end;
		
		KEEP prodotto %do j = 1 %to &sqlobs; &col&j %end;;
		output;
		
	run;
	%end;

	data APPOGGIO;
	set %do i = 1 %to &sqlobs; visualizza&i %end;;
	run;

	proc sql noprint;
	create table PERDITE_&campo&k.. as
	select distinct t2.aggregazione, t1.prodotto,
	%do i = 1 %to &sqlobs;
		%if &i = &sqlobs %then %do;
			sum(t1.&col&i) as &col&i
		%end;

		%else %do;
			sum(t1.&col&i) as &col&i, 
		%end;
	%end;
	from APPOGGIO t1
	left join DATI_DW.prodotto_canale t2 on (t1.prodotto = t2.prodotto_desc)
	group by t1.prodotto, t2.aggregazione
	;
	
	proc delete data = PERDITE_UOCE_AGGR APPOGGIO %do i = 1 %to &sqlobs; visualizza&i %end; ;
	run;
%end;

%mend;

%let path_variabili = /sas/staging/opt/sasop/pgm/SAS_standard/sascode/Poste_RAFA/excel_aggregazioni;

proc import OUT= var_frodi_reclami
            DATAFILE= "/sas/staging/opt/sasop/pgm/SAS_standard/sascode/Poste_RAFA/excel_aggregazioni/campi_perdite.xlsx" 
            DBMS=XLSX REPLACE;
     		GETNAMES=YES;
run;

proc sql noprint;
select distinct campo
into :campo1-
from var_frodi_reclami;
quit;

%let nvar = &sqlobs.;


%vari_join

%varie_aggregazioni

%perdite_frodi;

proc delete data = prodIn_cruscSettimanale;
run;
