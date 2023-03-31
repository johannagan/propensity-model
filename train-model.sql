
#Create Model
/*
This query creates a propensity model using logistic regression.
*/

/*
Classification Models:
- LOGISTIC_REG
- BOOSTED_TREE_CLASSIFIER
- RANDOM_FOREST_CLASSIFIER
- DNN_CLASSIFIER
- DNN_LINEAR_COMBINED_CLASSIFIER
*/



-- Train a logistic regression using BQML.
CREATE OR REPLACE MODEL `{my_model_L1_1}`
OPTIONS(
  MODEL_TYPE ='logistic_reg'
, INPUT_LABEL_COLS = ['labels']
, L1_REG = 1
, DATA_SPLIT_METHOD = 'RANDOM'
, DATA_SPLIT_EVAL_FRACTION = 0.20
) AS
SELECT * EXCEPT(fullVisitorId, first_conversion_session, last_conversion_timestamp, last_visit_timestamp, front_lookback_window, end_lookback_window)
FROM `{training_data_table}`
;


-- Review Training Info
SELECT * FROM ML.TRAINING_INFO(MODEL '{my_model}');
-- Review Model Weights
SELECT * FROM ML.WEIGHTS(MODEL `{my_model}`, STRUCT(TRUE
AS standardize)) ORDER by weight DESC;
-- Confusion Matrix
SELECT * FROM ML.CONFUSION_MATRIX(MODEL `{my_model}`);
-- ROC CURVE
SELECT * FROM ML.ROC_CURVE(MODEL `{my_model}`);
-- Evaluate Model
SELECT * FROM ML.EVALUATE(MODEL `{my_model}`);


-- Display model results in table
SELECT '0' AS L1, * FROM ML.EVALUATE(MODEL {my_model_L1_0})
UNION ALL
SELECT '1' AS L1, * FROM ML.EVALUATE(MODEL {my_model_L1_1})
UNION ALL
SELECT '10' AS L1, * FROM ML.EVALUATE(MODEL {my_model_L1_10})
UNION ALL
SELECT '50' AS L1, * FROM ML.EVALUATE(MODEL {my_model_L1_50})
UNION ALL
SELECT '100' AS L1, * FROM ML.EVALUATE(MODEL {my_model_L1_100})
UNION ALL
SELECT '1000' AS L1, * FROM ML.EVALUATE(MODEL {my_model_L1_1000})

