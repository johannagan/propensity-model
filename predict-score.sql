# Predict Query
/*
This query takes the model created and uses it to score new users.
The output is a record for every fullVisitorId with their purchase prediction probability.
*/

WITH probabilities AS (
    SELECT
        fullVisitorId,
        predicted_labels,
        predicted_labels_probs
    FROM
        ML.PREDICT(MODEL `{my_model_L1_1}`,
        (
        SELECT
            * EXCEPT(labels)
        FROM
            `{visitor_data_table}`
        )
    )
)

# Users ranked by conversion probability, grouped by decile
SELECT
  fullVisitorId,
  p.prob as probability,
  NTILE(10) OVER (ORDER BY p.prob DESC) as decile # sorting from highest to lowest probability
FROM
  probabilities, UNNEST(predicted_labels_probs) AS p
WHERE p.label = 1 # only purchasers