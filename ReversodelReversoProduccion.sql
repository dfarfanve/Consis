--------------OJO Actualiza todos los movimientos de la poliza
DECLARE  
    v_operationpk long;
    v_policyid long;

BEGIN
               
                
    FOR k in (
select distinct

       co.item,
       co.id as operacion,
       co.TIME_STAMP 
from app_vida.contextoperation co

join app_vida.agregatedpolicy ap on co.item = ap.agregatedpolicyid

join app_vida.product pro on ap.productid=pro.productid

join app_vida.eventdco ed on ed.operationpk = co.id and ed.policypk is not null

join app_vida.eventtype et on ed.eventtypeid = et.eventtypeid

join app_vida.policydco pdco on co.id=pdco.operationpk and co.status in (3)

join app_vida.prepolicy pp on pdco.dcoid = pp.pk

join app_vida.stpo_policyaslogical st on st.opk=co.id --and st.pal_status=1 

join app_vida.policydco pdco1 on pdco1.operationpk=ap.operationpk

join state st1 on pdco1.stateid=st1.stateid

--join app_vida.STPS_BATCHPROCESSRECORD_RBD RBD on RBD.ID_OBJECT= co.id 

--join APP_MIGRACION.TMP_CLAU_1 TEMP on TEMP.POLICYNUMBER=pp.CERTIFICADONUMEROINPUT --tabla temporal donde inserte los certificados a reversar

left join app_vida.openitem oi on oi.operationpk = co.id 

left join app_vida.stca_openiteminfo oii on oii.opm_id = oi.openitemid and oii.opmi_desc = 'NUMERODOCACSELX'

left join  (select tob.numdoc,tob.ideacsele from app_vida.transoblig tob where tob.stsoblig='VAL'

              and not exists (select 1 from transoblig tt where tob.numdoc=tt.numdoc and tt.stsoblig='ACT')

          ) tob on oi.openitemid=tob.ideacsele

left join transacre t on oii.opmi_value=t.numdoc and t.stsacre='VAL' and oii.opm_id=t.ideacsele

where
co.id=1485781257
  /*pp.numeropolizainput ='S-13484' and 
 RBD.FILENAME like '2019-08-%_RevertOperation' 
and RBD.TYPE ='DEV'*/
order by co.item, co.TIME_STAMP asc, co.id desc
       

) loop

      v_operationpk := k.operacion ;
      v_policyid := k.item;
     
     UPDATE policydco SET STATUS=1 
                WHERE OPERATIONPK=v_operationpk;
                
                UPDATE riskunitdco SET STATUS=1 
                WHERE OPERATIONPK=v_operationpk;
                
                UPDATE insuranceobjectdco SET STATUS=1 
                WHERE OPERATIONPK=v_operationpk;
                
                UPDATE coveragedco SET STATUS=1 
                WHERE OPERATIONPK=v_operationpk;
				
				UPDATE stpo_policyparticipationDCO SET STATUS=1 
                WHERE OPERATIONPK=v_operationpk;
				
                UPDATE STPO_INSOBJPARTICIPATIONDCO SET STATUS=1 
                WHERE OPERATIONPK=v_operationpk;
				
				 update app_vida.openitem oi set oi.status = 'active', oi.OPM_SUBSTATUS='active' where oi.OPERATIONPK=v_operationpk;
				 update app_vida.openitem oi set oi.status = 'applied', oi.OPM_SUBSTATUS='applied' where oi.OPERATIONPK
				 in (select o.OPERATIONPK from OPENITEM o where o.OPERATIONPK=v_operationpk and dty_id=8); -- si tiene pagados queda applied si no que queda active
                
               update contextoperation set STATUS=2  where ID=v_operationpk;
                
                update agregatedpolicy  set operationpk = v_operationpk,time_stamp = (select co.time_stamp from app_vida.contextoperation co where co.id = v_operationpk)   where agregatedpolicyid = v_policyid;
      
                update agregatedriskunit set operationpk = v_operationpk,time_stamp = (select co.time_stamp from app_vida.contextoperation co where co.id = v_operationpk)  
                where agregatedpolicyid = v_policyid; 
				
				update agregatedinsuranceobject set operationpk = v_operationpk,time_stamp = (select co.time_stamp from app_vida.contextoperation co where co.id = v_operationpk)
				  where agregatedriskunitid in  (
				   select agregatedriskunitid from agregatedriskunit where   agregatedpolicyid = v_policyid  );
       
			    update evaluatedcoverage set operationpk = v_operationpk where agregatedinsuranceobjectid in (
				select agregatedinsuranceobjectid from agregatedinsuranceobject where agregatedriskunitid in  (
				select agregatedriskunitid from agregatedriskunit where   agregatedpolicyid = v_policyid
				) );
                
                update stpo_policyparticipation set operationpk = v_operationpk,time_stamp = (select co.time_stamp from app_vida.contextoperation co where co.id = v_operationpk)   
				where AGREGATEDPARENTID = v_policyid;
                
                update stpo_insobjparticipation set operationpk = v_operationpk, time_stamp = (select co.time_stamp from app_vida.contextoperation co where co.id = v_operationpk) 
				where AGREGATEDPARENTID in (
				select agregatedinsuranceobjectid from agregatedinsuranceobject where agregatedriskunitid in  (
				select agregatedriskunitid from agregatedriskunit where   agregatedpolicyid = v_policyid
				) );
                update app_vida.stpo_policyaslogical set pal_status=1 where opk=v_operationpk ; --si son por trama, si no quitar
                delete  REVERSEDPOLICY where OPERATIONPK=v_operationpk ;
                
                delete APP_VIDA.REVERSEDPOLICY where operationpk=v_operationpk ;
               
    commit ; 
     
    end loop;
end;