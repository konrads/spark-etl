-- transform - item_purchase
SELECT i.id,
       i.name,
       totals.total_purchase
  FROM item i
       INNER JOIN
       (SELECT id,
               SUM(purchase) AS total_purchase
          FROM (SELECT typed_client.id,
                       (typed_item.price * typed_transaction.quantity) AS purchase
                  FROM (SELECT id, name, CAST(age AS INTEGER) FROM client) AS typed_client
                       LEFT OUTER JOIN
                       (SELECT client_id, item_id, CAST(quantity AS INTEGER) FROM transaction) AS typed_transaction
                       ON typed_transaction.client_id = typed_client.id
                       LEFT OUTER JOIN
                       (SELECT id, CAST(price AS INTEGER) FROM item) AS typed_item
                       ON typed_transaction.item_id = typed_item.id
               ) GROUP BY id
         ) AS totals
         ON i.id = totals.id
