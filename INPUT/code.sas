option mlogic;
data  join1;
	length  Unita_Org_Comp_Desc $256.
			Codice 8
			Prodotto_CODE    $ 24
			Event_Type_CODE  $ 24;

	if _n_=0 then
		set QUERY_FOR_RECUPERI2(keep=
            Data_Creazione
			Importo
			id_WF_Status_CODE
			evento_recod
			Data_Contabilizzazione
		);

	if _n_=1 then
		do;
			declare hash T2(hashexp:7, dataset:'dati_dw.eventi (where=(
				%_eg_WhereParam( VALID_FROM_DTTM, Prompt_data, LE, TYPE=DT, IS_EXPLICIT=0 ) AND 
				%_eg_WhereParam( VALID_TO_DTTM, Prompt_data, GE, TYPE=DT, IS_EXPLICIT=0 )))', multidata:'Y');
			T2.definekey('Codice');
			T2.definedata(  'Unita_Org_Comp_Desc', 
					        'Codice', 
					        'Event_Type_CODE', 
					        'Prodotto_CODE');
			T2.definedone();
			call missing (of _ALL_);
		end;

	set QUERY_FOR_RECUPERI2(keep=
            Data_Creazione
			Importo
			id_WF_Status_CODE
			evento_recod
			Data_Contabilizzazione
			where=(Data_Contabilizzazione IS MISSING AND 
			       Data_Creazione >= '1Jan2018:0:0:0'dt)
		);
	rc1=T2.find(key: evento_recod);
	drop data_creazione;
run;

data join2;
	length competenza $6. Prodotto_CODE $ 24;

	if _n_=0 then
		set join1;

	if _n_=1 then
		do;
			declare hash T3(dataset:'dati_dw.mappa_prodotti',  multidata:'Y');
			T3.definekey('Prodotto_CODE');
			T3.definedata('Competenza', 'Prodotto_CODE');
			T3.definedone();
		end;

	set join1(where=(Prodotto_CODE NOT IN 
					  (
			           '11',
			           '30',
			           '53',
			           '56',
			           '57',
			           '58',
			           '70',
			           '72',
			           '03',
			           '17',
			           '18',
			           '35',
			           '3',
			           '42',
			           '50',
			           '63',
			           '64',
			           '69',
			           '51',
			           '52'
			           ) AND
				      Event_Type_CODE ^= '08_04_01_05'));
	AnnoMeseCreaz = year(Data_Creazione)*100+month(Data_Creazione);
	rc1=T3.find();
run;

data join3;
	length Event_Type $24. cruscotto $ 64;

	if _n_=0 then
		set join2;

	if _n_=1 then
		do;
			declare hash T3(dataset:'dati_dw.COD_FRODI',  multidata:'Y');
			T3.definekey('Event_Type');
			T3.definedata('Event_Type', 'cruscotto');
			T3.definedone();
		end;

		set join2(where = (competenza = 'Imel_E'));
	rc1=T3.find(key: Event_Type_CODE);
run;


data  join1;
	length  Data_Creazione 8
			Importo 8
			id_WF_Status_CODE $ 64
			evento_recod 8;

	if _n_=0 then
		set dati_dw.eventi (keep=Unita_Org_Comp_Desc 
					        Codice 
					        Event_Type_CODE 
					        Prodotto_CODE);
	if _n_=1 then
		do;
			declare hash T2(hashexp:7, dataset:'QUERY_FOR_RECUPERI2(where=(Data_Contabilizzazione IS MISSING))', multidata:'Y');
			T2.definekey('evento_recod');
			T2.definedata(  'Data_Creazione',
							'Importo',
							'id_WF_Status_CODE',
							'evento_recod');
			T2.definedone();
			call missing (of _ALL_);
		end;

	set dati_dw.eventi (keep=Unita_Org_Comp_Desc 
					        Codice 
					        Event_Type_CODE 
					        Prodotto_CODE 
							VALID_FROM_DTTM
							VALID_TO_DTTM
						where=(
				%_eg_WhereParam( VALID_FROM_DTTM, Prompt_data, LE, TYPE=DT, IS_EXPLICIT=0 ) AND 
				%_eg_WhereParam( VALID_TO_DTTM, Prompt_data, GE, TYPE=DT, IS_EXPLICIT=0 ) AND
				Prodotto_CODE NOT IN 
			           (
			           '11',
			           '30',
			           '53',
			           '56',
			           '57',
			           '58',
			           '70',
			           '72',
			           '03',
			           '17',
			           '18',
			           '35',
			           '3',
			           '42',
			           '50',
			           '63',
			           '64',
			           '69',
			           '51',
			           '52'
			           ) AND
				Event_Type_CODE ^= '08_04_01_05'));
	rc1 = T2.find(key: codice);
	do while (rc1 = 0);
        output;
        rc1 = T2.find_next();
    end;
	format Data_Creazione datetime.;
	drop VALID_TO_DTTM
		 VALID_FROM_DTTM;
run;

data join2;
	length competenza $6. Prodotto_CODE $ 24;

	if _n_=0 then
		set join1;

	if _n_=1 then
		do;
			declare hash T3(dataset:'dati_dw.mappa_prodotti',  multidata:'Y');
			T3.definekey('Prodotto_CODE');
			T3.definedata('Competenza', 'Prodotto_CODE');
			T3.definedone();
		end;

	set join1(where=(Data_Creazione >= '1Jan2018:0:0:0'dt));
	AnnoMeseCreaz = year(datepart(Data_Creazione))*100+month(datepart(Data_Creazione));
	rc1=T3.find();
run;

data join3;
	length Event_Type $24. cruscotto $ 64;

	if _n_=0 then
		set join2;

	if _n_=1 then
		do;
			declare hash T3(dataset:'dati_dw.COD_FRODI',  multidata:'Y');
			T3.definekey('Event_Type');
			T3.definedata('Event_Type', 'cruscotto');
			T3.definedone();
		end;

		set join2(where = (competenza = 'Imel_E'));
	rc1=T3.find(key: Event_Type_CODE);
run;

data _null_;
	if 0 then
		do;
			set join3;
		end;

	if _N_=1 then
		do;
			declare hash T3(multidata:'Y',  hashexp: 20, ordered :'y' );
			T3.definekey(			   
			    'competenza',
                'AnnoMeseCreaz',
                'Unita_Org_Comp_Desc',
                'cruscotto',
                'id_WF_Status_CODE'
				);
			T3.definedata(
				'competenza', 
				'AnnoMeseCreaz',
				'SUM_of_Importo1',
				'Unita_Org_Comp_Desc', 
				'cruscotto', 
				'id_WF_Status_CODE');
			T3.definedone();
			before=input(getoption('xmrlmem'),20.);
			format before sizekmg10.2;
			put 'Hash Object before :' before;
		end;

	do until (eof);
		set join3 end = eof;

		if T3.find(
			    KEY: competenza,
                KEY: AnnoMeseCreaz,
                KEY: Unita_Org_Comp_Desc,
                KEY: cruscotto,
                KEY: id_WF_Status_CODE) ne 0 then
			do;
				SUM_of_importo1=0;
			end;

		SUM_of_Importo1 + Importo;
		T3.replace();
	end;

	after=input(getoption('xmrlmem'),20.);
	format after sizekmg10.2;
	put 'Hash Object after :' after;
	hashsize=before-after;
	rc = T3.output (dataset: "QUERY_FOR_RECUPERI_NV");
	put 'Hash Object Takes Up:' hashsize sizekmg10.2;
run;


