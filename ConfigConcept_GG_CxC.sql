Configurar concepto Gastos de Gestión y Costo por Cobertura por BDD
Se ingresa por Configuración, por default caera en la ultima linea, consultar el simbolo, y asignarlo en la linea con el orden deseado, y correr todo un valor mas


select * from APP_VIDA.STPT_FUNDCONFFORPLAN where fcfp_symbol = 'TipoRentaCC' ;

select * from APP_VIDA.STPT_FUNDCONFFORPLAN where fucp_id = 801 order by fcfp_order;

select * from APP_VIDA.STPT_FUNDCONFFORPLAN where fcfp_symbol = 'TipoRentaCC' ;

delete APP_VIDA.STPT_FUNDCONFFORPLAN where fcfp_symbol = 'TipoRentaCC' ;

update STPT_FUNDCONFFORPLAN set fcfp_order = fcfp_order+1  where fucp_id = 801 and  fcfp_order >1;

update STPT_FUNDCONFFORPLAN set fcfp_order = 2  where fcfp_symbol = 'TipoRentaCC' ;