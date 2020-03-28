/*
update STHC_ENDORSEMENTLETTER
set covi_invoice = null
 where covi_invoice in
(select element from  (
 with tbl(str) as (
             select replace(caph_record,'|','|')  from STPO_CLAIMASLPHYSICAL cph
where CPH.CAL_PK in
(select clg.CAL_PK from STPO_CLAIMASLOGICAL clg where clg.CALH_FILENAME ='FACTURASOAT20180808.xlsx')
        )
        select rownum-1 pos, regexp_substr(str, '(.*?)(\||$)', 1, level, null, 1) element
        from tbl
        connect by level <= regexp_count(str, '\|')+1) where pos = 20);
        
        
        
delete from STCL_COVERAGEINVOICEDETAIL  where covi_invoice in
(select element from  (
 with tbl(str) as (
             select replace(caph_record,'|','|')  from STPO_CLAIMASLPHYSICAL cph
where CPH.CAL_PK in
(select clg.CAL_PK from STPO_CLAIMASLOGICAL clg where clg.CALH_FILENAME ='FACTURASOAT20180808.xlsx')
        )
        select rownum-1 pos, regexp_substr(str, '(.*?)(\||$)', 1, level, null, 1) element
        from tbl
        connect by level <= regexp_count(str, '\|')+1) where pos = 20);


delete from STCL_COVERAGEINVOICE where covi_invoice in
(select element from  (
 with tbl(str) as (
             select replace(caph_record,'|','|')  from STPO_CLAIMASLPHYSICAL cph
where CPH.CAL_PK in
(select clg.CAL_PK from STPO_CLAIMASLOGICAL clg where clg.CALH_FILENAME = 'FACTURASOAT20180808.xlsx')
        )
        select rownum-1 pos, regexp_substr(str, '(.*?)(\||$)', 1, level, null, 1) element
        from tbl
        connect by level <= regexp_count(str, '\|')+1) where pos = 20);

commit;
*/

declare
--v_file varchar2(500) := 'TramaAltasSiniestrosSOAT_20180813.xls';
--v_file varchar2(500) := 'GARANTIASOATPruebaFacturas.xls';
--v_file varchar2(500) := 'FACTURASOAT1CG.xlsx';
v_file varchar2(500) := 'JLT-ProteccionCreditos-ALTAS_ENDOSOS-Prueba124.txt';


begin   
    
   delete from STPO_POLICYASLEDITHISTORY ph where PH.PAL_PK in
    (select LG.PAL_PK from stpo_policyaslogical lg where LG.PALH_FILENAME = v_file);

    delete from STPO_POLICYASLPHYSICAL ph where PH.PAL_PK in
    (select LG.PAL_PK from stpo_policyaslogical lg where LG.PALH_FILENAME = v_file);
    
    delete from STPO_POLICYASLHISTORY ph where PH.PAL_PK in
    (select LG.PAL_PK from stpo_policyaslogical lg where LG.PALH_FILENAME = v_file);

    delete from stpo_policyaslogical lg where LG.PALH_FILENAME = v_file;
    delete from STPO_POLICYASLHEADER he where upper(HE.PALH_FILENAME) = upper(v_file);
    
    delete  from STPO_CLAIMASLPHYSICAL cph
where CPH.CAL_PK in
(select clg.CAL_PK from STPO_CLAIMASLOGICAL clg where clg.CALH_FILENAME = v_file);

    delete from STPO_CLAIMASLOGICAL ch where CH.CALH_FILENAME = v_file;
    delete STPO_CLAIMASLHEADER ch where CH.CALH_FILENAME = v_file;
    
    
    commit;
end;
/*
delete  from STPO_CLAIMASLPHYSICAL cph
where CPH.CAL_PK in
(select clg.CAL_PK from STPO_CLAIMASLOGICAL clg where clg.CAL_STATUS = 0);


delete from STPO_CLAIMASLOGICAL lg
where lg.CAL_STATUS = 0

select count(*),lg.CAL_STATUS , lg.CALH_FILENAME from STPO_CLAIMASLOGICAL lg
where lg.CAL_STATUS = 0
group by lg.CAL_STATUS, lg.CALH_FILENAME;

select lg.CAL_PK from STPO_CLAIMASLOGICAL lg
where lg.CAL_STATUS = 0*/