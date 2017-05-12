-- transform - client_spending
SELECT c.id,
       c.name,
       totals.total_spending
  FROM client c
       INNER JOIN
       (SELECT id,
               SUM(spent) AS total_spending
          FROM (SELECT typed_client.id,
                       (typed_item.price * typed_transaction.quantity) AS spent
                  FROM (SELECT id, name FROM client) AS typed_client
                       LEFT OUTER JOIN
                       (SELECT client_id, item_id, CAST(quantity AS INTEGER) FROM transaction) AS typed_transaction
                       ON typed_transaction.client_id = typed_client.id
                       LEFT OUTER JOIN
                       (SELECT id, CAST(price AS INTEGER) FROM item) AS typed_item
                       ON typed_transaction.item_id = typed_item.id
               ) GROUP BY id
         ) AS totals
         ON c.id = totals.id
