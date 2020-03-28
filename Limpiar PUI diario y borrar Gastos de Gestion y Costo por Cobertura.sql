-- 1. Borrar del PUI
delete APP_VIDA.stli_fundmovementpui WHERE  FMP_ID in (
select FMP_ID  FROM APP_VIDA.STLI_FUNDMOVEMENTPOLICY  where FMP_PARENTID   in (
select FMP_ID  FROM APP_VIDA.STLI_FUNDMOVEMENTPOLICY  where apo_id in (501213491 )
 and fmp_parentid is null
-- and Trunc(fmp_dateoperation) = to_date('29-11-2019','dd-mm-yyyy')
-- and Trunc(fmp_dateoperation) <= to_date('30-11-2019','dd-mm-yyyy')
)
);

-- 2. borrar estos
 delete APP_VIDA.STLI_FUNDMOVEMENTPOLICY
--select *  from APP_VIDA.STLI_FUNDMOVEMENTPOLICY  
where FMP_PARENTID   in (
select  FMP_ID
FROM APP_VIDA.STLI_FUNDMOVEMENTPOLICY  where apo_id in ( 501213491)
-- and fmp_parentid is null
--  AND FMP_CONCEPT != 'GastoDeGestion'
-- and Trunc(fmp_dateoperation) = to_date('02-07-2018','dd-mm-yyyy')
-- and Trunc(fmp_dateoperation) = to_date('29-11-2019','dd-mm-yyyy')
-- and Trunc(fmp_dateoperation) <= to_date('30-11-2019','dd-mm-yyyy')
 and fmp_parentid is null

);

-- 3 Borrar origen  Solo si es para limpiar el Gasto de Gestion o Costo por Cobertura
delete  APP_VIDA.STLI_FUNDMOVEMENTPOLICY  
where apo_id in ( 501213491)
and fmp_source in (1,2)
--  and Trunc(fmp_dateoperation) = to_date('29-11-2019','dd-mm-yyyy')
and fmp_parentid is null  ;


-- 4. Validar
 select  * From
 APP_VIDA.STLI_FUNDMOVEMENTPOLICY
 where apo_id in ( 501213491)
--  and Trunc(fmp_dateoperation) = to_date('29-11-2019','dd-mm-yyyy')
and fmp_parentid is null  ;

delete  APP_VIDA.STLI_FUNDMOVEMENTPOLICY
 where apo_id in ( 561421714) and fmp_status = 1 and cor_id = 561421714;


-- Limpiar de la Batch de Gastos de Gestion (PUI MENSUAL)  y GASTOS DE GESTION

select * from app_vida.STPR_BATCHFUNDFORPLAN where APO_ID=561421714 and  trunc(bffp_processdate) = to_date('29-11-2019','dd-mm-yyyy') ;
select * from app_vida.STPR_BATCHFUNDCOST where APO_ID=561421714 and  trunc(bfc_processdate) = to_date('29-11-2019','dd-mm-yyyy') ;

delete app_vida.STPR_BATCHFUNDFORPLAN where APO_ID=501213491 and  trunc(bffp_processdate) = to_date('29-11-2019','dd-mm-yyyy') ;

delete app_vida.STPR_BATCHFUNDCOST where APO_ID=501213491 and  trunc(bfc_processdate) = to_date('29-11-2019','dd-mm-yyyy') ;