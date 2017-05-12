SELECT -- null checks
       (SELECT count(1) FROM client_spending WHERE id IS NULL)   = 0 AS id_null_less,
       (SELECT count(1) FROM client_spending WHERE name IS NULL) = 0 AS name_null_less,
       -- min checks
       min(id)             > 0                                       AS id_positive_ok,
       min(total_spending) > 0                                       AS total_spending_ok,
       -- col width checks
       max(length(name))  <= 10                                      AS name_ok
  FROM client_spending