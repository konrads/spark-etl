-- pre-check - transaction
SELECT -- null checks
      (SELECT count(1) FROM transaction WHERE client_id IS NULL)                                    = 0 AS client_id_null_less,
      (SELECT count(1) FROM transaction WHERE item_id IS NULL)                                      = 0 AS item_id_null_less,
      (SELECT count(1) FROM transaction WHERE quantity IS NULL)                                     = 0 AS quantity_null_less,
      -- positive price checks
      (SELECT count(1) FROM (SELECT CAST(quantity AS INTEGER) FROM transaction) WHERE quantity < 0) = 0 AS has_positive_quantity