-- pre-check - client
SELECT -- null checks
       (SELECT ${count_fun}(1) FROM client WHERE id IS NULL)   = 0 AS id_null_less,
       (SELECT ${count_fun}(1) FROM client WHERE name IS NULL) = 0 AS name_null_less,
       (SELECT ${count_fun}(1) FROM client WHERE age IS NULL)  = 0 AS age_null_less