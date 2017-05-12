SELECT -- null checks
       (SELECT count(1) FROM minor_purchase WHERE id IS NULL)   = 0 AS id_null_less,
       (SELECT count(1) FROM minor_purchase WHERE name IS NULL) = 0 AS name_null_less,
       -- min checks
       min(id)             > 0                                      AS id_positive_ok,
       min(sold_to_minors) > 0                                      AS sold_to_minors_ok,
       -- col width checks
       max(length(name))  <= 10                                     AS name_ok
  FROM minor_purchase