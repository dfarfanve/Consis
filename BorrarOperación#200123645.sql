declare 
    V_newoperationpk            number(19);
    V_newtimestamp              date;
    p_operationpktodel          number(19):= 186432824;
    v_numrows                   number(19);
    v_policyid                  number(19);
    v_template                  varchar2(250);
    v_templateid                number(19);
    v_status                    number(19);
    v_COLLECTIONTYPE            varchar2(250);
    v_COMMISSIONTYPE            varchar2(250);
    v_vartmp                    varchar2(2048);
    v_applied_status            varchar2(128) := 'applied1';
    v_count number;
    procedure delete_dco(p_template varchar2, p_templateid number, p_dcotable varchar, p_dcojoincolumn varchar, p_operationpk number) as
        v_predtemplate          varchar2(250);
    begin
        
        select max(COT.DESCRIPTION) into v_predtemplate from CONFIGURABLEOBJECTTYPE cot where COT.STATE = 0 and 
        exists (select * from CONFIGURABLEOBJECTTYPE incot where INCOT.CONFIGURABLEOBJECTTYPEID = p_templateid and COT.TYPE = INCOT.TYPE);
        
        if v_predtemplate is not null then
            execute immediate 'DELETE '||v_predtemplate||' TMPL WHERE EXISTS (SELECT * FROM '||p_dcotable||' dco WHERE dco.'||p_dcojoincolumn
                ||' = TMPL.PK AND dco.OPERATIONPK = :operationPk)' using p_operationpk;
            dbms_output.put_line('rows deleted in '||v_predtemplate||' '||sql%rowcount);
        end if;            
        
        if p_template is not null then
            execute immediate 'DELETE '||p_template||' TMPL WHERE EXISTS (SELECT * FROM '||p_dcotable||' dco WHERE dco.'||p_dcojoincolumn
            ||' = TMPL.PK AND dco.OPERATIONPK = :operationPk)' using p_operationpk;
            dbms_output.put_line('rows deleted in '||p_template||' '||sql%rowcount);
        end if;            
    end; 
     procedure delete_claim(p_policyid number) as
        v_claimid number;
    v_claimnumber varchar2(100) ;
    v_cnr_id number;
begin
  for jj in (select * from claim where policyid = p_policyid)   loop
v_claimid := jj.claimid;
--select claimid into v_claimid from claim where claimnumber =  v_claimnumber;

    for i in (select * from claimnormalreserve where
--              description = 'FALLECIMIENTO' and  
             claiminsuranceobjectid in
        (select claiminsuranceobjectid from CLAIMINSURANCEOBJECT where claimriskunitid in 
            (select claimriskunitid from claimriskunit where claimid= v_claimid))) loop
        v_cnr_id := i.CLAIMNORMALRESERVEDID;
        
       /* delete from RI_CLAIMOPERATIONS where por_id in (select paymentorderid from paymentorder where crbf_id in 
        (select crbf_id from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id));*/
        
        delete STAD_CLAIMCONTEXT where pod_id in (select paymentorderid from paymentorder where crbf_id in 
        (select crbf_id from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id));
        delete STCL_PAYMENTSTATE where por_id in (select paymentorderid from paymentorder where crbf_id in 
        (select crbf_id from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id));
        
        DELETE RI_CLAIMOPERATIONDETAILS DET WHERE DET.CLAIMOPERATIONID IN
        (SELECT CO.CLAIMOPERATIONID FROM RI_CLAIMOPERATIONS CO WHERE CO.POR_ID IN (select paymentorderid from paymentorder where crbf_id in 
        (select crbf_id from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id)));
        
        DELETE RI_CLAIMOPERATIONS WHERE POR_ID IN (select paymentorderid from paymentorder where crbf_id in 
        (select crbf_id from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id));
       
        delete STCL_PAYMENTSTATE where por_id in (select paymentorderid from paymentorder where crbf_id in 
        (select crbf_id from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id));
        
         delete from paymentorder where crbf_id in 
        (select crbf_id from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id);
        delete from STCA_CLAIMBENEFICIARY where crbf_id in 
        (select crbf_id from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id);
        dbms_output.put_line(v_cnr_id);
        
        delete from STCL_CLAIMRESERVEBENEFIT crb where CRB.CNR_ID = v_cnr_id;
        delete from STRI_OPHISTDETAILCLAIM where cnr_id = v_cnr_id;
        delete from STCL_RESERVESTATE where cnr_id = v_cnr_id;
        delete from CLAIMREQUISITE where cnr_id = v_cnr_id;
        delete from claimnormalreserve where CLAIMNORMALRESERVEDID = v_cnr_id;
    end loop;
delete from CLAIMINSURANCEOBJECT where claimriskunitid in 
(select claimriskunitid from claimriskunit where claimid= v_claimid);

delete from claimriskunit where claimid= v_claimid;
delete from STAD_CLAIMCONTEXT where cla_id = v_claimid;
delete from STCL_CLAIMOPERATIONHISTORY where claimid =v_claimid;
delete from STRP_CLAIMLETTERHISTORY where claimid =v_claimid;
delete from claim where claimid =v_claimid;
end loop;
end;
procedure delete_operation as
begin
    savepoint sv_pol;
   dbms_output.put_line('p_operationpktodel='||p_operationpktodel);
    begin
    
        select id, TIME_STAMP into V_newoperationpk,V_newtimestamp from
        (select ctx.id, CTX.TIME_STAMP, rank() over (partition by CTX.TIME_STAMP order by ctx.id asc) orden
          from contextoperation ctx where CTX.status = 2 and CTX.TIME_STAMP = 
            (select max(INCTX1.TIME_STAMP) from contextoperation inctx1, contextoperation inctx2 where INCTX2.ID = p_operationpktodel and INCTX1.STATUS = 2
            and INCTX1.ITEM = INCTX2.ITEM and INCTX1.ITEM = ctx.item and inctx1.id <> p_operationpktodel  /*and INCTX1.TIME_STAMP < INCTX2.TIME_STAMP*/)) where orden = 1;
    exception when no_data_found then
        V_newoperationpk := null;
        V_newtimestamp := null;
    end;
    
    begin
        select ctx.item, CTX.STATUS into v_policyid, v_status from contextoperation ctx where ctx.id = p_operationpktodel;
    exception when no_data_found then
        v_policyid := null;
        v_status := null;
    end;
    dbms_output.put_line('v_policyid= '||v_policyid||', v_status='||v_status);
    if v_status <> 2 then
       begin
       select ID,TIME_STAMP into V_newoperationpk,V_newtimestamp from
       (select OCTX.ID,OCTX.TIME_STAMP, rank() over (partition by octx.item order by octx.id desc) pos  from contextoperation octx where OCTX.STATUS = 2 and 
            OCTX.ITEM = v_policyid and OCTX.TIME_STAMP =
         (select max(CTX.TIME_STAMP) from contextoperation ctx where ctx.item = v_policyid and CTX.STATUS = 2)) where pos = 1;
--         select OCTX.ID,OCTX.TIME_STAMP into V_newoperationpk,V_newtimestamp from contextoperation octx where OCTX.STATUS = 2 and OCTX.ITEM = v_policyid and OCTX.TIME_STAMP =
--         (select max(CTX.TIME_STAMP) from contextoperation ctx where ctx.item = v_policyid and CTX.STATUS = 2);
        exception when no_data_found then
            V_newoperationpk := null;
            V_newtimestamp :=null;
        end;         
    end if;
      
    dbms_output.put_line('V_newoperationpk= '||V_newoperationpk||', V_newtimestamp='||V_newtimestamp);
    
    -- Inicio Coberturas
    select max(COT.DESCRIPTION), max(COT.CONFIGURABLEOBJECTTYPEID) into v_template, v_templateid from evaluatedcoverage ec, coveragedco cd, configuratedcoverage cc, CONFIGURABLEOBJECTTYPE cot 
    where CD.AGREGATEDOBJECTID = EC.EVALUATEDCOVERAGEID and CD.OPERATIONPK = p_operationpktodel
    and CC.CONFIGURATEDCOVERAGEID = EC.CONFIGURATEDCOVERAGEID and COT.CONFIGURABLEOBJECTTYPEID = CC.TEMPLATEID;
    
    delete_dco(v_template, v_templateid, 'coveragedco', 'dcoid', p_operationpktodel);
 
    delete coveragedco cd where CD.OPERATIONPK = p_operationpktodel;
    dbms_output.put_line('rows deleted in coveragedco '||sql%rowcount);

    select count(*) into v_numrows from evaluatedcoverage ec where EC.OPERATIONPK = p_operationpktodel;

    IF V_NUMROWS > 0 THEN
        IF V_NEWOPERATIONPK IS NOT NULL THEN
            UPDATE EVALUATEDCOVERAGE EC SET EC.OPERATIONPK = V_NEWOPERATIONPK, EC.TIME_STAMP =V_newtimestamp WHERE EC.OPERATIONPK= P_OPERATIONPKTODEL;
            dbms_output.put_line('rows updated in evaluatedcoverage '||sql%rowcount);
        END IF;
    END IF;

    -- Fin coberturas

    -- Inicio plan de financiamiento
    SELECT MAX(AFP.COLLECTIONTYPE), MAX(AFP.COMMISSIONTYPE) INTO V_COLLECTIONTYPE, V_COMMISSIONTYPE 
    FROM FINANCIALPLANDCO FPD, AGREGATEDFINANCIALPLAN AFP WHERE 
    FPD.AGREGATEDOBJECTID = AFP.AGREGATEDFINANCIALPLANID AND FPD.OPERATIONPK = P_OPERATIONPKTODEL;

    BEGIN
        SELECT COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID INTO V_TEMPLATE,V_TEMPLATEID FROM CONFIGURABLEOBJECTTYPE COT WHERE 
            COT.CONFIGURABLEOBJECTTYPEID = V_COLLECTIONTYPE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        V_TEMPLATE := NULL;
        V_TEMPLATEID := NULL;
    END;
    
    DELETE_DCO(V_TEMPLATE, V_TEMPLATEID, 'financialplandco', 'DCOIDCOLLECTION', P_OPERATIONPKTODEL);

    BEGIN
        SELECT COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID INTO V_TEMPLATE, V_TEMPLATEID FROM CONFIGURABLEOBJECTTYPE COT WHERE 
        COT.CONFIGURABLEOBJECTTYPEID = V_COMMISSIONTYPE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        V_TEMPLATE := NULL;
        V_TEMPLATEID := NULL;
    END;
    
    DELETE_DCO(V_TEMPLATE, V_TEMPLATEID, 'FINANCIALPLANDCO', 'DCOIDCOMMISSION', P_OPERATIONPKTODEL);

    DELETE FINANCIALPLANDCO FPD WHERE FPD.OPERATIONPK = P_OPERATIONPKTODEL;
    dbms_output.put_line('rows deleted in financialplandco '||sql%rowcount);

    SELECT COUNT(*) INTO V_NUMROWS FROM AGREGATEDFINANCIALPLAN AFP WHERE AFP.OPERATIONPK = P_OPERATIONPKTODEL;
        
    IF V_NUMROWS > 0 THEN
        IF V_NEWOPERATIONPK IS NOT NULL THEN
            UPDATE AGREGATEDFINANCIALPLAN AFP SET AFP.OPERATIONPK = V_NEWOPERATIONPK, AFP.TIME_STAMP = V_newtimestamp WHERE AFP.OPERATIONPK= P_OPERATIONPKTODEL;
            dbms_output.put_line('rows updated in agregatedfinancialplan '||sql%rowcount);
        END IF;
    END IF;        
    
    -- Fin plan de financiamiento
    
    
    
    -- Inicio de objetos asegurados

    FOR I IN (SELECT COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID FROM CONFIGURABLEOBJECTTYPE COT, STPO_INSOBJPARTICIPATIONDCO IOPD, STPO_INSOBJPARTICIPATION IOP WHERE IOP.AGREGATEDOBJECTID = IOPD.AGREGATEDOBJECTID
        AND COT.CONFIGURABLEOBJECTTYPEID = IOP.TYPEID AND IOPD.OPERATIONPK = P_OPERATIONPKTODEL GROUP BY COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID)
    LOOP
        V_TEMPLATE := I.DESCRIPTION;
        V_TEMPLATEID := I.CONFIGURABLEOBJECTTYPEID;
        DELETE_DCO(V_TEMPLATE, V_TEMPLATEID, 'STPO_INSOBJPARTICIPATIONDCO', 'DCOID', P_OPERATIONPKTODEL);
    END LOOP;
    
    delete STPO_INSOBJPARTICIPATIONDCO iopd where IOPD.OPERATIONPK =  p_operationpktodel;
    dbms_output.put_line('rows deleted in STPO_INSOBJPARTICIPATIONDCO '||sql%rowcount);
    
    select count(*) into v_numrows from STPO_INSOBJPARTICIPATION iop where iop.OPERATIONPK = p_operationpktodel;
    
    if v_numrows > 0 then
        if V_newoperationpk is not null then
            update STPO_INSOBJPARTICIPATION iop set iop.OPERATIONPK = V_newoperationpk, IOP.TIME_STAMP= V_newtimestamp where iop.OPERATIONPK= p_operationpktodel;
        end if;
        dbms_output.put_line('rows updated in STPO_INSOBJPARTICIPATION '||sql%rowcount);
    END IF;
    
    delete STPO_surchargesDCO iopd where IOPD.cor_id =  p_operationpktodel;
    dbms_output.put_line('rows deleted in STPO_surchargesDCO '||sql%rowcount);
    
    select count(*) into v_numrows from STPO_SURCHARGES iop where iop.cor_id = p_operationpktodel;
    
    if v_numrows > 0 then
        if V_newoperationpk is not null then
            update STPO_SURCHARGES iop set iop.cor_id = V_newoperationpk/*, IOP.TIME_STAMP= V_newtimestamp*/ where iop.cor_id= p_operationpktodel;
        end if;
        dbms_output.put_line('rows updated in STPO_SURCHARGES '||sql%rowcount);
    END IF;
    
    
    
    BEGIN
        SELECT max(COT.DESCRIPTION), max(COT.CONFIGURABLEOBJECTTYPEID) INTO V_TEMPLATE, V_TEMPLATEID FROM INSURANCEOBJECTDCO IOD, AGREGATEDINSURANCEOBJECT AIO, CONFIGURABLEOBJECTTYPE COT WHERE 
        IOD.AGREGATEDOBJECTID = AIO.AGREGATEDINSURANCEOBJECTID AND COT.CONFIGURABLEOBJECTTYPEID = AIO.TYPEID AND IOD.OPERATIONPK = P_OPERATIONPKTODEL; 
    EXCEPTION WHEN NO_DATA_FOUND THEN
        V_TEMPLATE := NULL;
        V_TEMPLATEID := NULL;
    END;
    
    DELETE_DCO(V_TEMPLATE, V_TEMPLATEID, 'INSURANCEOBJECTDCO', 'DCOID', P_OPERATIONPKTODEL);

    delete insuranceobjectdco iod where IOD.OPERATIONPK = p_operationpktodel;
    dbms_output.put_line('rows deleted in insuranceobjectdco '||sql%rowcount);

    select count(*) into v_numrows from AGREGATEDINSURANCEOBJECT aio where aio.OPERATIONPK = p_operationpktodel;
    if v_numrows > 0 then
        if V_newoperationpk is not null then
            update AGREGATEDINSURANCEOBJECT aio set aio.OPERATIONPK = V_newoperationpk, AIO.TIME_STAMP = V_newtimestamp where aio.OPERATIONPK= p_operationpktodel;
        end if; 
        dbms_output.put_line('rows updated in AGREGATEDINSURANCEOBJECT '||sql%rowcount);           
    end if;

    -- Fin de objetos asegurados

    -- Inicio de unidades de riesgo
    
    FOR I IN (SELECT COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID FROM CONFIGURABLEOBJECTTYPE COT, STPO_RUPARTICIPATIONDCO URPD, STPO_RUPARTICIPATION URP WHERE URP.AGREGATEDOBJECTID = URPD.AGREGATEDOBJECTID
    AND COT.CONFIGURABLEOBJECTTYPEID = URP.TYPEID AND URPD.OPERATIONPK = P_OPERATIONPKTODEL GROUP BY COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID)
    LOOP
        V_TEMPLATE := I.DESCRIPTION;
        V_TEMPLATEID := I.CONFIGURABLEOBJECTTYPEID;
        DELETE_DCO(V_TEMPLATE, V_TEMPLATEID, 'STPO_RUPARTICIPATIONDCO', 'DCOID', P_OPERATIONPKTODEL);
    END LOOP;

    delete STPO_RUPARTICIPATIONDCO rud where rud.OPERATIONPK =  p_operationpktodel;
    dbms_output.put_line('rows deleted in STPO_RUPARTICIPATIONDCO '||sql%rowcount);
    
    select count(*) into v_numrows from STPO_RUPARTICIPATION rup where rup.OPERATIONPK = p_operationpktodel;
    
    if v_numrows > 0 then
        if V_newoperationpk is not null then
            update STPO_RUPARTICIPATION rup set rup.OPERATIONPK = V_newoperationpk, RUP.TIME_STAMP = V_newtimestamp where rup.OPERATIONPK= p_operationpktodel;
        end if;
        dbms_output.put_line('rows updated in STPO_RUPARTICIPATION '||sql%rowcount);
    END IF;


    BEGIN
        SELECT max(COT.DESCRIPTION), max(COT.CONFIGURABLEOBJECTTYPEID) INTO V_TEMPLATE, V_TEMPLATEID FROM RISKUNITDCO RUD, AGREGATEDRISKUNIT ARU, CONFIGURABLEOBJECTTYPE COT WHERE 
        RUD.AGREGATEDOBJECTID = ARU.AGREGATEDRISKUNITID AND COT.CONFIGURABLEOBJECTTYPEID = ARU.TYPEID AND RUD.OPERATIONPK = P_OPERATIONPKTODEL;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        V_TEMPLATE := NULL;
        V_TEMPLATEID := NULL;
    END;         

    DELETE_DCO(V_TEMPLATE, V_TEMPLATEID, 'RISKUNITDCO', 'DCOID', P_OPERATIONPKTODEL);

    DELETE RISKUNITDCO RUD WHERE RUD.OPERATIONPK = P_OPERATIONPKTODEL;
    dbms_output.put_line('rows deleted in RISKUNITDCO '||sql%rowcount);

    SELECT COUNT(*) INTO V_NUMROWS FROM AGREGATEDRISKUNIT ARU WHERE ARU.OPERATIONPK = P_OPERATIONPKTODEL;
    IF V_NUMROWS > 0 THEN
        IF V_NEWOPERATIONPK IS NOT NULL THEN
            UPDATE AGREGATEDRISKUNIT ARU SET ARU.OPERATIONPK = V_NEWOPERATIONPK, ARU.TIME_STAMP =V_newtimestamp WHERE ARU.OPERATIONPK= P_OPERATIONPKTODEL;
        END IF;   
        dbms_output.put_line('rows updated in AGREGATEDRISKUNIT '||sql%rowcount);         
    END IF;

    -- Fin de unidades de riesgo
 
   /*  delete from STRI_OPHISTDETAILPREMIUM ophdp where OPHDP.ROHD_ID in 
    (select OPHD.ROHD_ID from   STRI_OPERATIONHISTORYDETAIL ophd where exists
    (select 1 from  STRI_OPERATIONHISTORY oph
        inner join (select * from (select ROP.*,CTX.ID operationpk,CTX.TIME_STAMP, rank() over(partition by ROP.OPERATIONID order by CTX.TIME_STAMP asc) orden1,
            rank() over(partition by ctx.id order by ROP.OPERATIONID asc) orden2 from reinsuranceoperation rop 
            inner join contextoperation ctx on CTX.ITEM = ROP.AGREGATEDPOLICYID and ctx.status = 2 and trunc(CTX.TIME_STAMP) = ROP.RO_DATE
            inner join policydco pd on PD.OPERATIONPK = ctx.id and PD.INITIALDATE = ROP.INITIALDATE and PD.FINISHDATE = ROP.FINALDATE 
            inner join eventdco ed on ED.OPERATIONPK = ctx.id
            inner join eventtype et on ET.EVENTTYPEID = ED.EVENTTYPEID and ET.DESCRIPTION = ROP.POLICYEVENT
            where ctx.id = p_operationpktodel
            order by ROP.OPERATIONID desc) where orden1 = orden2) rop on ROP.OPERATIONID =OPH.ROP_ID 
     where  OPH.ROH_ID = OPHD.ROH_ID) );
    dbms_output.put_line('rows deleted in STRI_OPHISTDETAILPREMIUM '||sql%rowcount);

    
    delete from  STRI_OPERATIONHISTORYDETAIL ophd 
    where exists
    (select 1 from  STRI_OPERATIONHISTORY oph
     inner join (select * from    
            (select ROP.*,CTX.ID operationpk,CTX.TIME_STAMP, rank() over(partition by ROP.OPERATIONID order by CTX.TIME_STAMP asc) orden1,
            rank() over(partition by ctx.id order by ROP.OPERATIONID asc) orden2 from reinsuranceoperation rop 
            inner join contextoperation ctx on CTX.ITEM = ROP.AGREGATEDPOLICYID and ctx.status = 2 and trunc(CTX.TIME_STAMP) = ROP.RO_DATE
            inner join policydco pd on PD.OPERATIONPK = ctx.id and PD.INITIALDATE = ROP.INITIALDATE and PD.FINISHDATE = ROP.FINALDATE 
            inner join eventdco ed on ED.OPERATIONPK = ctx.id
            inner join eventtype et on ET.EVENTTYPEID = ED.EVENTTYPEID and ET.DESCRIPTION = ROP.POLICYEVENT
            where ctx.id = p_operationpktodel
            order by ROP.OPERATIONID desc) where orden1 = orden2) rop on ROP.OPERATIONID = OPH.ROP_ID 
     where  OPH.ROH_ID = OPHD.ROH_ID);
    dbms_output.put_line('rows deleted in STRI_OPERATIONHISTORYDETAIL '||sql%rowcount);

    
     delete from STRI_OPERATIONHISTORY oph where exists
    (select 1 from (select * from    
            (select ROP.*,CTX.ID operationpk,CTX.TIME_STAMP, rank() over(partition by ROP.OPERATIONID order by CTX.TIME_STAMP asc) orden1,
            rank() over(partition by ctx.id order by ROP.OPERATIONID asc) orden2 from reinsuranceoperation rop 
            inner join contextoperation ctx on CTX.ITEM = ROP.AGREGATEDPOLICYID and ctx.status = 2 and trunc(CTX.TIME_STAMP) = ROP.RO_DATE
            inner join policydco pd on PD.OPERATIONPK = ctx.id and PD.INITIALDATE = ROP.INITIALDATE and PD.FINISHDATE = ROP.FINALDATE 
            inner join eventdco ed on ED.OPERATIONPK = ctx.id
            inner join eventtype et on ET.EVENTTYPEID = ED.EVENTTYPEID and ET.DESCRIPTION = ROP.POLICYEVENT
            where ctx.id = p_operationpktodel
            order by ROP.OPERATIONID desc) where orden1 = orden2) rop where ROP.OPERATIONID = OPH.ROP_ID);
    dbms_output.put_line('rows deleted in STRI_OPERATIONHISTORY '||sql%rowcount);
    
    delete from STRI_OPEDETDISTRIBUTION where ROD_ID in
     (select REINSURANCEOPERATIONDETAILID  from REINSURANCEOPERATIONDETAIL rod where exists
    (select 1 from  REINSURANCEOPERATIONCOMPONENT roc
    inner join REINSURANCEOPERATIONGROUPINFO rog on ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    inner join (select * from    
            (select ROP.*,CTX.ID operationpk,CTX.TIME_STAMP, rank() over(partition by ROP.OPERATIONID order by CTX.TIME_STAMP asc) orden1,
            rank() over(partition by ctx.id order by ROP.OPERATIONID asc) orden2 from reinsuranceoperation rop 
            inner join contextoperation ctx on CTX.ITEM = ROP.AGREGATEDPOLICYID and ctx.status = 2 and trunc(CTX.TIME_STAMP) = ROP.RO_DATE
            inner join policydco pd on PD.OPERATIONPK = ctx.id and PD.INITIALDATE = ROP.INITIALDATE and PD.FINISHDATE = ROP.FINALDATE 
            inner join eventdco ed on ED.OPERATIONPK = ctx.id
            inner join eventtype et on ET.EVENTTYPEID = ED.EVENTTYPEID and ET.DESCRIPTION = ROP.POLICYEVENT
            where ctx.id = p_operationpktodel
            order by ROP.OPERATIONID desc) where orden1 = orden2) rop on ROP.OPERATIONID = ROG.OPERATIONID
    where ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID));
    dbms_output.put_line('rows deleted in STRI_OPEDETDISTRIBUTION '||sql%rowcount);
    
  
     delete REINSURANCEOPERATIONDETAIL rod where exists
    (select 1 from  REINSURANCEOPERATIONCOMPONENT roc
    inner join REINSURANCEOPERATIONGROUPINFO rog on ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    inner join (select * from    
            (select ROP.*,CTX.ID operationpk,CTX.TIME_STAMP, rank() over(partition by ROP.OPERATIONID order by CTX.TIME_STAMP asc) orden1,
            rank() over(partition by ctx.id order by ROP.OPERATIONID asc) orden2 from reinsuranceoperation rop 
            inner join contextoperation ctx on CTX.ITEM = ROP.AGREGATEDPOLICYID and ctx.status = 2 and trunc(CTX.TIME_STAMP) = ROP.RO_DATE
            inner join policydco pd on PD.OPERATIONPK = ctx.id and PD.INITIALDATE = ROP.INITIALDATE and PD.FINISHDATE = ROP.FINALDATE 
            inner join eventdco ed on ED.OPERATIONPK = ctx.id
            inner join eventtype et on ET.EVENTTYPEID = ED.EVENTTYPEID and ET.DESCRIPTION = ROP.POLICYEVENT
            where ctx.id = p_operationpktodel
            order by ROP.OPERATIONID desc) where orden1 = orden2) rop on ROP.OPERATIONID = ROG.OPERATIONID
    where ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID);
    dbms_output.put_line('rows deleted in REINSURANCEOPERATIONDETAIL '||sql%rowcount);
    
     
     delete from REINSURANCEOPERATIONCOMPONENT roc where exists
    (select 1 from REINSURANCEOPERATIONGROUPINFO rog
    inner join (select * from    
            (select ROP.*,CTX.ID operationpk,CTX.TIME_STAMP, rank() over(partition by ROP.OPERATIONID order by CTX.TIME_STAMP asc) orden1,
            rank() over(partition by ctx.id order by ROP.OPERATIONID asc) orden2 from reinsuranceoperation rop 
            inner join contextoperation ctx on CTX.ITEM = ROP.AGREGATEDPOLICYID and ctx.status = 2 and trunc(CTX.TIME_STAMP) = ROP.RO_DATE
            inner join policydco pd on PD.OPERATIONPK = ctx.id and PD.INITIALDATE = ROP.INITIALDATE and PD.FINISHDATE = ROP.FINALDATE 
            inner join eventdco ed on ED.OPERATIONPK = ctx.id
            inner join eventtype et on ET.EVENTTYPEID = ED.EVENTTYPEID and ET.DESCRIPTION = ROP.POLICYEVENT
            where ctx.id = p_operationpktodel
            order by ROP.OPERATIONID desc) where orden1 = orden2) rop on ROP.OPERATIONID = ROG.OPERATIONID
    where  ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID);
    dbms_output.put_line('rows deleted in REINSURANCEOPERATIONCOMPONENT '||sql%rowcount);

      
     delete REINSURANCEOPERATIONGROUPINFO rog where exists
    (select 1 from (select * from    
            (select ROP.*,CTX.ID operationpk,CTX.TIME_STAMP, rank() over(partition by ROP.OPERATIONID order by CTX.TIME_STAMP asc) orden1,
            rank() over(partition by ctx.id order by ROP.OPERATIONID asc) orden2 from reinsuranceoperation rop 
            inner join contextoperation ctx on CTX.ITEM = ROP.AGREGATEDPOLICYID and ctx.status = 2 and trunc(CTX.TIME_STAMP) = ROP.RO_DATE
            inner join policydco pd on PD.OPERATIONPK = ctx.id and PD.INITIALDATE = ROP.INITIALDATE and PD.FINISHDATE = ROP.FINALDATE 
            inner join eventdco ed on ED.OPERATIONPK = ctx.id
            inner join eventtype et on ET.EVENTTYPEID = ED.EVENTTYPEID and ET.DESCRIPTION = ROP.POLICYEVENT
            where ctx.id = p_operationpktodel
            order by ROP.OPERATIONID desc) where orden1 = orden2) rop where ROP.OPERATIONID = ROG.OPERATIONID);
    dbms_output.put_line('rows deleted in REINSURANCEOPERATIONGROUPINFO '||sql%rowcount);

     
    delete from REINSURANCEOPERATION rop where ROP.OPERATIONID in (select operationid from    
            (select ROP.*,CTX.ID operationpk,CTX.TIME_STAMP, rank() over(partition by ROP.OPERATIONID order by CTX.TIME_STAMP asc) orden1,
            rank() over(partition by ctx.id order by ROP.OPERATIONID asc) orden2 from reinsuranceoperation rop 
            inner join contextoperation ctx on CTX.ITEM = ROP.AGREGATEDPOLICYID and ctx.status = 2 and trunc(CTX.TIME_STAMP) = ROP.RO_DATE
            inner join policydco pd on PD.OPERATIONPK = ctx.id and PD.INITIALDATE = ROP.INITIALDATE and PD.FINISHDATE = ROP.FINALDATE 
            inner join eventdco ed on ED.OPERATIONPK = ctx.id
            inner join eventtype et on ET.EVENTTYPEID = ED.EVENTTYPEID and ET.DESCRIPTION = ROP.POLICYEVENT
            where ctx.id = p_operationpktodel
            order by ROP.OPERATIONID desc) where orden1 = orden2);
    dbms_output.put_line('rows deleted in REINSURANCEOPERATION '||sql%rowcount);*/

delete from STRI_OPHISTDETAILPREMIUM ophdp where OPHDP.ROHD_ID in 
    (select OPHD.ROHD_ID from   STRI_OPERATIONHISTORYDETAIL ophd where exists
    (select 1 from  STRI_OPERATIONHISTORY oph
     where  OPH.ROH_ID = OPHD.ROH_ID
     and OPH.ROP_ID  in (select OPERATIONID from REINSURANCEOPERATION where  cor_id = p_operationpktodel)) );
    dbms_output.put_line('rows deleted in STRI_OPHISTDETAILPREMIUM '||sql%rowcount);
    
    
    delete from STRI_OPHISTDETAILCLAIM where ROHD_ID in
     (select ROHD_ID from  STRI_OPERATIONHISTORYDETAIL ophd 
    where exists
    (select 1 from  STRI_OPERATIONHISTORY oph
     where  OPH.ROH_ID = OPHD.ROH_ID
     and OPH.ROP_ID  in (select OPERATIONID from REINSURANCEOPERATION where  cor_id = p_operationpktodel)));
    
    delete from  STRI_OPERATIONHISTORYDETAIL ophd 
    where exists
    (select 1 from  STRI_OPERATIONHISTORY oph
     where  OPH.ROH_ID = OPHD.ROH_ID
     and OPH.ROP_ID  in (select OPERATIONID from REINSURANCEOPERATION where  cor_id = p_operationpktodel));
    dbms_output.put_line('rows deleted in STRI_OPERATIONHISTORYDETAIL '||sql%rowcount);

     delete from STRI_OPERATIONHISTORY oph where 
     OPH.ROP_ID  in (select OPERATIONID from REINSURANCEOPERATION where  cor_id = p_operationpktodel) ;
    dbms_output.put_line('rows deleted in STRI_OPERATIONHISTORY '||sql%rowcount);
    
    delete from STRI_OPEDETDISTRIBUTION where ROD_ID in
     (select REINSURANCEOPERATIONDETAILID  from REINSURANCEOPERATIONDETAIL rod where exists
    (select 1 from  REINSURANCEOPERATIONCOMPONENT roc
    inner join REINSURANCEOPERATIONGROUPINFO rog on ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    and ROG.OPERATIONID in     (select OPERATIONID from REINSURANCEOPERATION where  cor_id = p_operationpktodel)            
    where ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID));
    dbms_output.put_line('rows deleted in STRI_OPEDETDISTRIBUTION '||sql%rowcount);
    
    
  
     delete REINSURANCEOPERATIONDETAIL rod where exists
    (select 1 from  REINSURANCEOPERATIONCOMPONENT roc
    inner join REINSURANCEOPERATIONGROUPINFO rog on ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    where ROC.REINOPERCOMPONENTID = ROD.REINOPERCOMPONENTID
    and ROG.OPERATIONID in (select OPERATIONID from REINSURANCEOPERATION where  cor_id = p_operationpktodel));
    dbms_output.put_line('rows deleted in REINSURANCEOPERATIONDETAIL '||sql%rowcount);
    
     
     delete from REINSURANCEOPERATIONCOMPONENT roc where exists
    (select 1 from REINSURANCEOPERATIONGROUPINFO rog
    where  ROC.REINSURANCEGROUPINFOID = ROG.REINSURANCEGROUPINFOID
    and ROG.OPERATIONID in   (select OPERATIONID from REINSURANCEOPERATION where  cor_id = p_operationpktodel));
    dbms_output.put_line('rows deleted in REINSURANCEOPERATIONCOMPONENT '||sql%rowcount);

 delete REINSURANCEOPERATIONGROUPINFO rog where ROG.OPERATIONID in
  (select OPERATIONID from REINSURANCEOPERATION where  cor_id = p_operationpktodel);
    dbms_output.put_line('rows deleted in REINSURANCEOPERATIONGROUPINFO '||sql%rowcount);

   delete from REINSURANCEOPERATION where  cor_id = p_operationpktodel;
    dbms_output.put_line('rows deleted in REINSURANCEOPERATION '||sql%rowcount);
    
    
    
    
    
    
    -- Fin de operaciones de reaseguro

-- Inicio de movimientos financieros
    
    DELETE STPE_MOVEMENTUAADETAIL MOV WHERE MOV.OPERATIONPK = P_OPERATIONPKTODEL;
    dbms_output.put_line('rows deleted in STPE_MOVEMENTUAADETAIL '||sql%rowcount);
    
   DELETE UAADETAIL UAA WHERE EXISTS (SELECT * FROM OPENITEM INOPM WHERE INOPM.OPERATIONPK = P_OPERATIONPKTODEL AND
    INOPM.OPENITEMID = UAA.OPENITEMID AND INOPM.STATUS <> v_applied_status);
    dbms_output.put_line('rows deleted in UAADETAIL '||sql%rowcount);
    
    DELETE UAADETAIL UAA WHERE exists (SELECT * FROM OPENITEM OPM, OPENITEM INOPM WHERE OPM.PARENTOPENITEMID = INOPM.OPENITEMID 
     and INOPM.OPERATIONPK = P_OPERATIONPKTODEL AND INOPM.STATUS <> v_applied_status and OPM.OPENITEMID = UAA.OPENITEMID);

--    delete from SAMP.INTERMEDIO inter where exists (select * from openitem opm where OPM.OPENITEMID = INTER.ID_OPENITEM and OPM.DTY_ID = 7572
--    and OPM.OPERATIONPK = p_operationpktodel);
--    dbms_output.put_line('SAMP.INTERMEDIO '||sql%rowcount);
    
  /*  DELETE FROM EXT_LOGINTERFZSAMP EXT WHERE EXISTS (SELECT * FROM OPENITEM OPM WHERE OPM.OPENITEMID = EXT.OPENITEMID AND OPM.DTY_ID = 7572
    AND OPM.OPERATIONPK = P_OPERATIONPKTODEL);
    dbms_output.put_line('rows deleted in EXT_LOGINTERFZSAMP '||sql%rowcount);
    
    DELETE FROM EXT_INTERFZSAMP EXT WHERE EXISTS (SELECT * FROM OPENITEM OPM WHERE OPM.OPENITEMID = EXT.OPENITEMID AND OPM.DTY_ID = 7572
    AND OPM.OPERATIONPK = P_OPERATIONPKTODEL);
    dbms_output.put_line('rows deleted in EXT_INTERFZSAMP '||sql%rowcount);*/
    
   /* DELETE STCA_OPENITEMHISTORY oph WHERE exists (SELECT * FROM OPENITEM opm WHERE opm.OPERATIONPK = P_OPERATIONPKTODEL
     AND opm.STATUS <> v_applied_status and oph.OPM_ID = opm.OPENITEMID);
    dbms_output.put_line('rows deleted in STCA_OPENITEMHISTORY '||sql%rowcount);*/
    
--    DELETE STCA_OPENITEMHISTORY oph WHERE exists (SELECT * FROM OPENITEM OPM, OPENITEM INOPM WHERE OPM.PARENTOPENITEMID = INOPM.OPENITEMID 
--    and INOPM.OPERATIONPK = P_OPERATIONPKTODEL AND INOPM.STATUS <> v_applied_status and oph.OPM_ID = OPM.OPENITEMID);
    
    /*DELETE OPENITEMREFERENCE opr WHERE exists (SELECT * FROM OPENITEM opm WHERE opm.OPERATIONPK = P_OPERATIONPKTODEL
     AND opm.STATUS <> v_applied_status and opr.OPENITEMID=opm.OPENITEMID);
    dbms_output.put_line('rows deleted in OPENITEMREFERENCE '||sql%rowcount);*/
    
--    DELETE OPENITEMREFERENCE opr WHERE exists (SELECT * FROM OPENITEM OPM, OPENITEM INOPM WHERE OPM.PARENTOPENITEMID = INOPM.OPENITEMID 
--    and INOPM.OPERATIONPK = P_OPERATIONPKTODEL AND OPM.STATUS <> v_applied_status and opr.OPENITEMID = OPM.OPENITEMID);
    
  /*  delete from PAYMENTOPERATIONOPENITEM where 
        openitemid in (select openitemid from openitem where appliedto in (select openitemid from OPENITEM OPM
         WHERE OPM.OPERATIONPK = P_OPERATIONPKTODEL AND OPM.STATUS <> v_applied_status));

    delete from PAYMENTOPERATIONOPENITEM where 
        openitemid in (select openitemid from OPENITEM OPM WHERE OPM.OPERATIONPK = P_OPERATIONPKTODEL AND OPM.STATUS <> v_applied_status);
    
    delete OPENITEMWARNINGCOLLECTION where
        openitemid in (select openitemid from OPENITEM OPM WHERE OPM.OPERATIONPK = P_OPERATIONPKTODEL AND OPM.STATUS <> v_applied_status);
    
    delete MOVEMENTSENT where
    openitemid in (select openitemid from OPENITEM OPM WHERE OPM.OPERATIONPK = P_OPERATIONPKTODEL AND OPM.STATUS <> v_applied_status);
    
  delete STPR_BATCHPROCESSOPENITEM where
        opm_id in (select openitemid from OPENITEM OPM WHERE OPM.OPERATIONPK = P_OPERATIONPKTODEL AND OPM.STATUS <> v_applied_status);*/
       
    delete from openitem where appliedto in (select openitemid from OPENITEM OPM WHERE OPM.OPERATIONPK = P_OPERATIONPKTODEL 
    AND OPM.STATUS <> v_applied_status);

    DELETE OPENITEM OPM WHERE OPM.OPERATIONPK = P_OPERATIONPKTODEL AND OPM.STATUS <> v_applied_status;
    dbms_output.put_line('rows deleted in openitem '||sql%rowcount);
    
    DELETE FROM ENTRY ENT WHERE EXISTS (SELECT * FROM THIRDPARTYMOVEMENTENTRY TME, THIRDPARTYMOVEMENTPOLICY TMP WHERE TME.IDENTRY = ENT.PK
    AND TMP.OPERATIONPK = P_OPERATIONPKTODEL AND TMP.PK = TME.IDMOVEMENT);
    dbms_output.put_line('rows deleted in ENTRY '||sql%rowcount);
    
    DELETE THIRDPARTYMOVEMENTENTRY TME WHERE EXISTS (SELECT * FROM THIRDPARTYMOVEMENTPOLICY TMP WHERE TMP.OPERATIONPK = P_OPERATIONPKTODEL
     AND TMP.PK = TME.IDMOVEMENT);
    dbms_output.put_line('rows deleted in THIRDPARTYMOVEMENTENTRY '||sql%rowcount);

    DELETE THIRDPARTYMOVEMENTRISKUNIT TMR WHERE TMR.OPERATIONPK= P_OPERATIONPKTODEL;
    dbms_output.put_line('rows deleted in THIRDPARTYMOVEMENTRISKUNIT '||sql%rowcount);
    
    DELETE THIRDPARTYMOVEMENTPOLICY TMP WHERE TMP.OPERATIONPK= P_OPERATIONPKTODEL;
    dbms_output.put_line('rows deleted in THIRDPARTYMOVEMENTPOLICY '||sql%rowcount);
        
    -- Fin de movimientos financieros
    FOR I IN (SELECT COT.DESCRIPTION,  COT.CONFIGURABLEOBJECTTYPEID FROM CONFIGURABLEOBJECTTYPE COT, STPO_POLICYPARTICIPATIONDCO PPD, STPO_POLICYPARTICIPATION PP WHERE PP.AGREGATEDOBJECTID = PPD.AGREGATEDOBJECTID
    AND COT.CONFIGURABLEOBJECTTYPEID = PP.TYPEID AND PPD.OPERATIONPK = P_OPERATIONPKTODEL GROUP BY COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID)
    LOOP
        V_TEMPLATE := I.DESCRIPTION;
        V_TEMPLATEID := I.CONFIGURABLEOBJECTTYPEID;
        DELETE_DCO(V_TEMPLATE, V_TEMPLATEID, 'STPO_POLICYPARTICIPATIONDCO', 'DCOID', P_OPERATIONPKTODEL);
    END LOOP;

    delete STPO_POLICYPARTICIPATIONDCO ppd where ppd.OPERATIONPK = p_operationpktodel;
    dbms_output.put_line('rows deleted in STPO_POLICYPARTICIPATIONDCO '||sql%rowcount);
    
    SELECT COUNT(*) INTO V_NUMROWS FROM STPO_POLICYPARTICIPATION PP WHERE PP.OPERATIONPK = P_OPERATIONPKTODEL;
    IF V_NUMROWS > 0 THEN
        IF V_NEWOPERATIONPK IS NOT NULL THEN
            UPDATE STPO_POLICYPARTICIPATION PP SET PP.OPERATIONPK = V_NEWOPERATIONPK, PP.TIME_STAMP = V_newtimestamp WHERE PP.OPERATIONPK= P_OPERATIONPKTODEL;
        END IF;   
        dbms_output.put_line('rows updated in STPO_POLICYPARTICIPATION '||sql%rowcount);         
    END IF;
    
    begin
    SELECT COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID INTO V_TEMPLATE, v_templateid FROM EVENTDCO ED, EVENTTYPE ET, CONFIGURABLEOBJECTTYPE COT
     WHERE ET.EVENTTYPEID = ED.EVENTTYPEID AND ED.OPERATIONPK = p_operationpktodel AND COT.CONFIGURABLEOBJECTTYPEID = ET.EVENTFORMTYPEID
     and ET.OPERATIONTYPEID is not null;
    exception when no_data_found then
        v_template := null;
        v_templateid := null;
    end;
    
    delete_dco(v_template, v_templateid, 'EVENTDCO', 'IDDCOEVENT', p_operationpktodel);

    delete eventdco ed where ED.OPERATIONPK = p_operationpktodel;
    dbms_output.put_line('rows deleted in eventdco '||sql%rowcount); 
    
    v_vartmp := null;
    
    for c in (select * from stad_policycontext pct where PCT.CTO_OPK   = p_operationpktodel)
    loop
        if v_vartmp is not null then
            v_vartmp := v_vartmp ||',';
        end if; 
        v_vartmp := v_vartmp ||c.adt_id;
    end loop;
    
    
    delete stad_policycontext pct where PCT.CTO_OPK   = p_operationpktodel;
    dbms_output.put_line('rows deleted in stad_policycontext '||sql%rowcount); 
    
    if v_vartmp is not null then 
        execute immediate 'delete STAD_AUDITTRAIL adt where ADT.ADT_ID in ('||v_vartmp||') and ADT_TYPE in (1)';
        dbms_output.put_line('rows deleted in STAD_AUDITTRAIL '||sql%rowcount);
        --dbms_output.put_line('v_vartmp '||v_vartmp);
    end if;        

    -- Inicio operaciones de reseguro

  

    begin
        SELECT COT.DESCRIPTION, COT.CONFIGURABLEOBJECTTYPEID INTO V_TEMPLATE, v_templateid FROM POLICYDCO PD, AGREGATEDPOLICY AP, CONFIGURABLEOBJECTTYPE COT WHERE AP.AGREGATEDPOLICYID = PD.AGREGATEDOBJECTID AND 
        COT.CONFIGURABLEOBJECTTYPEID = AP.TYPEID AND PD.OPERATIONPK = p_operationpktodel;
    exception when others then
        v_template := null;
        v_templateid := null;
    end;        
    
    delete_dco(v_template, v_templateid, 'POLICYDCO', 'DCOID', p_operationpktodel);

    delete policydco pd where PD.OPERATIONPK =  p_operationpktodel;
    dbms_output.put_line('rows deleted in policydco '||sql%rowcount);
    
    delete from STRP_POLICYLETTERHISTORY where opk_id = p_operationpktodel; 
    dbms_output.put_line('rows deleted in STRP_POLICYLETTERHISTORY '||sql%rowcount);
    
    select count(*) into v_numrows from AGREGATEDPOLICY apo where apo.OPERATIONPK = p_operationpktodel;
    if v_numrows > 0 then
        if V_newoperationpk is not null then
            update AGREGATEDPOLICY apo set apo.OPERATIONPK = V_newoperationpk, APO.TIME_STAMP = V_newtimestamp where apo.OPERATIONPK= p_operationpktodel;
            dbms_output.put_line('rows updated in AGREGATEDPOLICY '||sql%rowcount);
        else null;
            delete_claim(v_policyid);
            delete from STLI_RESERVEHISTORY where cov_id in
            ( select EC.EVALUATEDCOVERAGEID from evaluatedcoverage ec where exists
            (select * from AGREGATEDINSURANCEOBJECT aio, AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID
            and AIO.AGREGATEDINSURANCEOBJECTID = EC.AGREGATEDINSURANCEOBJECTID));
            
            delete from coveragedco cd where cd.agregatedobjectid in
            (select ec.evaluatedcoverageid from evaluatedcoverage ec where exists
            (select * from AGREGATEDINSURANCEOBJECT aio, AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID
            and AIO.AGREGATEDINSURANCEOBJECTID = EC.AGREGATEDINSURANCEOBJECTID));
            
            
            delete evaluatedcoverage ec where exists
            (select * from AGREGATEDINSURANCEOBJECT aio, AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID
            and AIO.AGREGATEDINSURANCEOBJECTID = EC.AGREGATEDINSURANCEOBJECTID);
        
            delete STPO_PHDIO ph where exists
            (select * from STPO_INSOBJPARTICIPATION iop, AGREGATEDINSURANCEOBJECT aio, AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where 
            APO.AGREGATEDPOLICYID= v_policyid and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and
            ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID and AIO.AGREGATEDINSURANCEOBJECTID = IOP.AGREGATEDPARENTID
             and IOP.AGREGATEDOBJECTID = ph.ppa_id);
            
              
             delete from stpo_insobjparticipationdco ppd where ppd.agregatedobjectid in
             ( select iop.agregatedobjectid from STPO_INSOBJPARTICIPATION iop where exists
            (select * from AGREGATEDINSURANCEOBJECT aio, AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID
            and AIO.AGREGATEDINSURANCEOBJECTID = IOP.AGREGATEDPARENTID));
               
            DELETE STPO_INSOBJPARTICIPATION iop where exists
            (select * from AGREGATEDINSURANCEOBJECT aio, AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID
            and AIO.AGREGATEDINSURANCEOBJECTID = IOP.AGREGATEDPARENTID);
            
            delete STPO_SUBSREQUIREMENT req where exists
            (select * from AGREGATEDINSURANCEOBJECT aio, AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID
            and AIO.AGREGATEDINSURANCEOBJECTID = req.aio_id);
                 
            
            delete from insuranceobjectdco iod where iod.agregatedobjectid in
            (select aio.agregatedinsuranceobjectid from AGREGATEDINSURANCEOBJECT aio where exists            
            (select * from  AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID));
                   
            delete AGREGATEDINSURANCEOBJECT aio where exists            
            (select * from  AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = aio.AGREGATEDRISKUNITID);

            DELETE STPO_RUPARTICIPATION RUp where exists
            (select * from AGREGATEDRISKUNIT aru, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID and ARU.AGREGATEDRISKUNITID = RUP.AGREGATEDPARENTID);

            delete from riskunitdco rud where rud.agregatedobjectid in
            (select aru.agregatedriskunitid from AGREGATEDRISKUNIT aru  where exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID));    
            
            delete from openitem where openitemid in (select openitemid from openitemreference where riskunitid in
              (select aru.agregatedriskunitid from AGREGATEDRISKUNIT aru  where exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID)));  
            
            
            delete AGREGATEDRISKUNIT aru  where exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and aru.AGREGATEDPOLICYID = APO.AGREGATEDPOLICYID);
        
            delete STPO_POLICYPARTICIPATION pp where pp.OPERATIONPK= p_operationpktodel;
           
            delete STCI_COINSURANCEPARTDCO where cipa_id in
            (select cipa_id from STCI_COINSURANCEPARTICIPATION cicp where exists    
            (select * from sTCI_COINSURANCECONTRACT cic, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and cic.apo_id = APO.AGREGATEDPOLICYID and CIC.CIC_ID = cicp.cic_id));
            
            delete STCI_COINSURANCEPARTICIPATION cicp where exists    
            (select * from sTCI_COINSURANCECONTRACT cic, AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and cic.apo_id = APO.AGREGATEDPOLICYID and CIC.CIC_ID = cicp.cic_id);
        
            delete from STCI_COINSURANCECONTRACTDCO where cic_id in
            (select cic_id from sTCI_COINSURANCECONTRACT cic where exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and cic.apo_id = APO.AGREGATEDPOLICYID));
                
             delete from sTCI_COINSURANCECONTRACT cic where exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and cic.apo_id = APO.AGREGATEDPOLICYID);
            
            delete STRI_INSUREDCUMULUS insc where exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and insc.pol_id = APO.AGREGATEDPOLICYID);

            delete STRI_OPERATIONHISTORY oph where exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and oph.pol_id = APO.AGREGATEDPOLICYID);
            
            delete STPS_RELATIONAGREEMENTPOLICY rap where exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and RAP.RSA_POLICYID = APO.AGREGATEDPOLICYID);
            
            
            DELETE STRP_POLICYQUOTATIONINFO PQI WHERE exists (select * from AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid
            and PQI.POL_ID = APO.AGREGATEDPOLICYID);
           
            delete STRI_FACULTATIVEPOLICY where pol_id = v_policyid;
            delete from financialplandco fpd where fpd.agregatedobjectid in 
            (select afp.agregatedfinancialplanid from agregatedfinancialplan afp where AFP.CONTAINERID= v_policyid);
            
            DELETE agregatedfinancialplan afp where AFP.CONTAINERID= v_policyid;
            delete STLI_POLRESERVEMOVEMENT where pol_id = v_policyid;
            --delete STPR_BATCHPROCPERIODIPOLICY  where apo_id = v_policyid;
            delete STNO_NOTIFICATION  where apo_id = v_policyid;
            delete STPO_CUMULUS where pol_id = v_policyid;
            delete STLI_POLRESERVEHISTORY where pol_id = v_policyid;
            delete STPR_BATCHPROCPERIODIPOLICY where apo_id = v_policyid;   
            delete STPO_INSUREDCUMULUS where apo_id = v_policyid;     
            delete MOVEMENTSENT        where policyid =  v_policyid;     
            delete STAD_POLICYCONTEXT where agp_id =  v_policyid; 
            DELETE STPO_WILDCARDQUOTAHISTORY WHERE WQUO_ID IN
            (SELECT WQUO_ID FROM STPO_WILDCARDQUOTA where apo_id = v_policyid);   
            delete    STPR_BATCHPROCESSPOLICYRI where apo_id = v_policyid; 
            
            delete from openitem where openitemid in (select openitemid from openitemreference where policyid =  v_policyid);  
            delete STPS_LIFEEXTENDEDINFO where policy_id = v_policyid;                         
            delete STPO_WILDCARDQUOTA where apo_id = v_policyid;   
            delete STPO_PENDINGFACULTATIVE where apo_id = v_policyid;   
            delete AGREGATEDPOLICY apo where APO.AGREGATEDPOLICYID= v_policyid;
        end if;            
    end if;
    
   --DELETE FROM "+AcseleConf.getProperty("Entry.Table")+ " E 
   
    delete STEN_ENTRYEVENTRELATION erl where  exists(select * from STEN_ENTRYEVENT eev where  ERL.EEV_ID = EEV.EEV_ID 
    and EEV.EEV_OPERATIONID = p_operationpktodel);
    dbms_output.put_line('rows deleted in STEN_ENTRYEVENTRELATION '||sql%rowcount);
    
    delete STEN_ENTRYEVENT eev where EEV.EEV_OPERATIONID = p_operationpktodel;
    dbms_output.put_line('rows deleted in STEN_ENTRYEVENT '||sql%rowcount);

    delete sTPO_ENTRYGENERATIONHISTORY egh where EGH.EGH_OPERATIONPK = p_operationpktodel;
   dbms_output.put_line('rows deleted in sTPO_ENTRYGENERATIONHISTORY '||sql%rowcount);    

    delete STRP_POLICYQUOTATIONINFO where cor_id = p_operationpktodel;
    dbms_output.put_line('rows deleted in STRP_POLICYQUOTATIONINFO '||sql%rowcount);
    
    delete STPO_PENDINGFACULTATIVE where cor_id = p_operationpktodel;
    dbms_output.put_line('rows deleted in STPO_PENDINGFACULTATIVE '||sql%rowcount);
  
    /*delete from financialplandco fpd where fpd.agregatedobjectid in 
            (select afp.agregatedfinancialplanid from agregatedfinancialplan afp where afp.operationpk = p_operationpktodel);
  
    delete from agregatedfinancialplan afp where afp.operationpk = p_operationpktodel;
    dbms_output.put_line('rows deleted in agregatedfinancialplan '||sql%rowcount);*/
     delete STPO_SUBSREQUIREMENT where cor_id =    p_operationpktodel;
     
     delete STPO_GUARANTEEDVALUESDETAIL where gvt_id in 
     (select gvt_id from STPO_GUARANTEEDVALUESTABLE where cor_id =    p_operationpktodel);
     
     delete STPO_GUARANTEEDVALUESTABLE where cor_id =    p_operationpktodel;
     delete STNO_NOTIFICATION where cor_id =    p_operationpktodel;
     
     
    delete STPR_BATCHPROCESSPOLICYRI where cor_id =    p_operationpktodel;
    DELETE STPO_POLICYXMLARCHIVE WHERE  cor_id =    p_operationpktodel;
    DELETE REVERSEDPOLICY WHERE OPERATIONPK  = p_operationpktodel;
    delete STPR_BATCHFUTUREENDORSEMENT WHERE  cor_id =    p_operationpktodel;
    delete STPO_DELETEDCOVERAGE  WHERE  cor_id =    p_operationpktodel;
   -- delete from EXT_PAIDUPINFO where operationpk =  p_operationpktodel;
--    delete from EXT_RESCUEINFO where operationpk =  p_operationpktodel;
--    delete from EXT_EXTENDEDTERMINFO where operationpk =  p_operationpktodel;
    delete contextoperation ctx where ctx.id  = p_operationpktodel;
    dbms_output.put_line('rows deleted in contextoperation '||sql%rowcount);
    commit;
end;

begin
  delete_operation;
end;