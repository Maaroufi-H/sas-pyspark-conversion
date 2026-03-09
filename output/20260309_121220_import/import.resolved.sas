%include "/sas/staging/opt/sasop/pgm/SAS_standard/sascode/Poste_RAFA/autoexec.sas";

%macro import(flusso);

	%if %sysfunc(exist(result.register)) = 0 %then %do;

		data result.register;
		attrib computing_datetime length = 8 format = datetime26.; 
		attrib fase length = $50;
		attrib msg_error length = $150;
		attrib tipo length = $32; 
		attrib flusso length = $32;
		stop;
		run;

	%end;

data register;
	length computing_datetime 8 fase $50 msg_error $150 tipo $32 flusso $32;
	computing_datetime = "&timest_for."dt;
	fase = "INIZIO: CARICAMENTO DATI.";
	msg_error = "";
	tipo = "ACQUISIZIONE";
	flusso = "&flusso.";
	format computing_datetime datetime26.;
	run;	
		
	proc append base=result.register data=register;
	run;


	%inc "&path./source/Importa_metadati.sas";
	%inc "&path./source/Importa_liste.sas";


	%inc "&path./source/Acquisisci_flussi.sas";

	%if &syscc > 4 %then %do;

		options obs=max nosyntaxcheck;

		%mail_errore(import, flusso = &flusso);
		
		%if %sysfunc(exist(result.register)) = 0 %then %do;

			data result.register;
			attrib computing_datetime length = 8 format = datetime26.; 
			attrib fase length = $50;
			attrib msg_error length = $150;
			attrib tipo length = $32; 
			attrib flusso length = $32;
			stop;
			run;

		%end;

		data register;
		length computing_datetime 8 fase $50 msg_error $150 tipo $32 flusso $32;
		computing_datetime = "&timest_for."dt;
		fase = "FINE: CARICAMENTO DATI.";
		msg_error = "ERROR: errore durante il caricamento dei file.";
		tipo = "ACQUISIZIONE";
		flusso = "&flusso.";
		format computing_datetime datetime26.;
		run;	
		
		proc append base=result.register data=register;
		run;



	%end;

	%if &syscc le 4 and &num_t > 0 %then %do;

		%mail_caricamento(&flusso.);

		%if %sysfunc(exist(result.register)) = 0 %then %do;

			data result.register;
			attrib computing_datetime length = 8 format = datetime26.; 
			attrib fase length = $50;
			attrib msg_error length = $150;
			attrib tipo length = $32; 
			attrib flusso length = $32;
			stop;
			run;

		%end;

		data register;
		length computing_datetime 8 fase $50 msg_error $150 tipo $32 flusso $32;
		computing_datetime = "&timest_for."dt;
		fase = "FINE: CARICAMENTO DATI.";
		msg_error = "";
		tipo = "ACQUISIZIONE";
		flusso = "&flusso.";
		format computing_datetime datetime26.;
		run;	
		
		proc append base=result.register data=register;
		run;

		%inc "&path./source/aggrega_flussi.sas";

	%end;

	proc datasets lib = work nolist kill;
	run;
	quit;

%mend;



