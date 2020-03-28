select numcert, count(1)  FROM INT_ANULACION_AX@DBL_PROD  where codprod in  (8809, 8501, 8801, 8711 )
  and stsretornoae = 1 group by numcert having count(1) > 1;