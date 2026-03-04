%macro filtra_prodotti(nid, id_prodotto1, id_prodotto2, id_prodotto3);

proc sql;
    create table work.risultato as
    select *
    from source.vendite
    where prodotto_cod in (
        %if &nid = 1 %then %do;
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
    and data_vendita >= '2024-01-01';
quit;

%mend filtra_prodotti;
