-- pre-check - item
SELECT -- null checks
      (SELECT count(1) FROM item WHERE id IS NULL)                                     = 0 AS id_null_less,
      (SELECT count(1) FROM item WHERE name IS NULL)                                   = 0 AS name_null_less,
      (SELECT count(1) FROM item WHERE price IS NULL)                                  = 0 AS age_null_less,
      (SELECT count(1) FROM item WHERE for_adults IS NULL)                             = 0 AS for_adults_null_less,
      -- positive price checks
      (SELECT count(1) FROM (SELECT CAST(price AS INTEGER) FROM item) WHERE price < 0) = 0 AS has_positive_prices