-- transform - minor_purchase
SELECT i.id,
       i.name,
       totals.sold_to_minors
  FROM item i
       INNER JOIN
       (SELECT id,
               SUM(sold_to_minors) AS sold_to_minors
          FROM (SELECT typed_client.id,
                       positive_transaction.sold_to_minors
                  FROM (SELECT id, CAST(age AS INTEGER) FROM client) AS typed_client
                       INNER JOIN
                       (SELECT client_id, item_id, quantity AS sold_to_minors FROM (SELECT client_id, item_id, CAST(quantity AS INTEGER) FROM transaction) WHERE quantity > 0) AS positive_transaction
                       ON positive_transaction.client_id = typed_client.id
                       INNER JOIN
                       item
                       ON positive_transaction.item_id = item.id
               ) GROUP BY id
         ) AS totals
         ON i.id = totals.id
